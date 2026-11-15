import type { INestApplication, LoggerService } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "../../../src/app.module.ts";
import { getTestOrm } from "./database.ts";

export interface HttpResult<T = any> {
  status: number;
  body: T;
  headers: Headers;
}

export interface TestApp {
  url: string;
  app: INestApplication;
  get<T = any>(path: string, headers?: Record<string, string>): Promise<HttpResult<T>>;
  post<T = any>(path: string, body: unknown, headers?: Record<string, string>): Promise<HttpResult<T>>;
  close(): Promise<void>;
}

/**
 * Sobe a aplicação INTEIRA (mesmo AppModule de produção) numa porta aleatória.
 * listen(0): o sistema operacional escolhe uma porta livre, então o teste nunca
 * briga com o `bun run dev` e dá para subir várias instâncias ao mesmo tempo.
 */
export async function startTestApp(options: { logger?: LoggerService | false } = {}): Promise<TestApp> {
  await getTestOrm(); // garante o schema antes do Nest conectar

  // Sem logs por padrão (a saída dos testes fica limpa). Quem quer ler os logs
  // passa um logger próprio (ex.: JsonLogger escrevendo num array).
  const app = await NestFactory.create(AppModule, { logger: options.logger ?? false });
  await app.listen(0);
  const url = (await app.getUrl()).replace("[::1]", "localhost");

  const request = async <T>(path: string, init?: RequestInit): Promise<HttpResult<T>> => {
    const res = await fetch(`${url}${path}`, init);
    const text = await res.text();
    return { status: res.status, body: (text ? JSON.parse(text) : undefined) as T, headers: res.headers };
  };

  return {
    url,
    app,
    get: (path, headers = {}) => request(path, { headers }),
    post: (path, body, headers = {}) =>
      request(path, {
        method: "POST",
        headers: { "content-type": "application/json", ...headers },
        body: JSON.stringify(body),
      }),
    close: () => app.close(),
  };
}
