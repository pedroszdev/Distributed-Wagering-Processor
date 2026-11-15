import { resolve } from "node:path";
import type { Subprocess } from "bun";

// import.meta.dir (e não URL.pathname): o nome da pasta tem "—", que viraria %E2%80%94.
const PROJECT_ROOT = resolve(import.meta.dir, "../../..");

/** Uma porta livre agora (o SO escolhe; fechamos e devolvemos o número). */
export function freePort(): number {
  const server = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = server.port!;
  server.stop(true);
  return port;
}

export interface AppProcess {
  url: string;
  process: Subprocess;
  /** SIGKILL: morte súbita, sem shutdown (simula queda da máquina/OOM). */
  kill(): Promise<void>;
  /** SIGTERM: shutdown normal. Devolve o exit code. */
  stop(): Promise<number | null>;
}

/**
 * Sobe a aplicação como um PROCESSO SEPARADO (`bun src/main.ts`), como em produção.
 * Diferente do startTestApp (Nest dentro do processo do teste), este pode ser
 * morto com SIGKILL no meio de uma transação: o teste de verdade de "o processo
 * morre antes/depois do commit".
 */
export async function startAppProcess(env: Record<string, string> = {}): Promise<AppProcess> {
  const port = freePort();
  const url = `http://localhost:${port}`;
  // process.execPath: o mesmo binário do Bun que está rodando os testes (não depende do PATH).
  const child = Bun.spawn([process.execPath, "src/main.ts"], {
    cwd: PROJECT_ROOT,
    env: { ...process.env, NODE_ENV: "test", PORT: String(port), LOG_LEVEL: "error", ...env },
    stdout: "ignore",
    stderr: "inherit",
  });

  const deadline = Date.now() + 20_000;
  for (;;) {
    try {
      if ((await fetch(`${url}/health/live`)).ok) break;
    } catch {
      // ainda subindo
    }
    if (child.exitCode !== null) throw new Error(`a aplicação saiu ao subir (código ${child.exitCode})`);
    if (Date.now() > deadline) {
      child.kill("SIGKILL");
      throw new Error("a aplicação não respondeu /health/live a tempo");
    }
    await Bun.sleep(100);
  }

  return {
    url,
    process: child,
    kill: async () => {
      child.kill("SIGKILL");
      await child.exited;
    },
    stop: async () => {
      child.kill("SIGTERM");
      return child.exited;
    },
  };
}
