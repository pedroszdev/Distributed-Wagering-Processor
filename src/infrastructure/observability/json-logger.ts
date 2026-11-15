import type { LoggerService, LogLevel } from "@nestjs/common";
import { logContext } from "./log-context.ts";

/** Do mais detalhado ao mais grave. */
const LEVELS: LogLevel[] = ["verbose", "debug", "log", "warn", "error", "fatal"];

/** Nome do nível na saída JSON (convenção comum: "info" em vez do "log" do Nest). */
const OUTPUT_LEVEL: Record<LogLevel, string> = {
  verbose: "trace",
  debug: "debug",
  log: "info",
  warn: "warn",
  error: "error",
  fatal: "fatal",
};

/**
 * Chaves que NUNCA saem no log (seção 12: "sem dados sensíveis ou payloads
 * financeiros completos"). Rede de segurança: quem loga já não deve mandar isso.
 */
const REDACTED_KEYS = new Set(["authorization", "password", "secret", "token", "body", "payload", "money", "input"]);

export type LogWriter = (line: string) => void;

/**
 * Logger do Nest que escreve UMA linha JSON por log, no stdout:
 *
 *   {"time":"…","level":"info","context":"SqsWagerTransactionConsumer",
 *    "msg":"mensagem processada","correlationId":"…","messageId":"…",
 *    "transactionId":"…","walletId":"…","providerId":"…","durationMs":12}
 *
 * Os campos do logContext (correlationId etc.) entram sozinhos em toda linha.
 * Uma linha por log = fácil de coletar e filtrar (Loki, CloudWatch, Datadog…).
 */
export class JsonLogger implements LoggerService {
  private readonly minLevel: number;

  constructor(
    minLevel: LogLevel = "log",
    private readonly write: LogWriter = (line) => process.stdout.write(`${line}\n`),
    private readonly base: Record<string, unknown> = { service: "wagering", pid: process.pid },
  ) {
    this.minLevel = LEVELS.indexOf(minLevel);
  }

  log(message: unknown, ...params: unknown[]): void {
    this.emit("log", message, params);
  }
  warn(message: unknown, ...params: unknown[]): void {
    this.emit("warn", message, params);
  }
  debug(message: unknown, ...params: unknown[]): void {
    this.emit("debug", message, params);
  }
  verbose(message: unknown, ...params: unknown[]): void {
    this.emit("verbose", message, params);
  }
  fatal(message: unknown, ...params: unknown[]): void {
    this.emit("fatal", message, params);
  }
  /** O Nest chama error(msg, stack?, context?). */
  error(message: unknown, ...params: unknown[]): void {
    this.emit("error", message, params);
  }

  private emit(level: LogLevel, message: unknown, params: unknown[]): void {
    if (LEVELS.indexOf(level) < this.minLevel) return;

    // Convenção do Nest: o ÚLTIMO parâmetro string é o contexto (nome da classe);
    // em error(), um parâmetro string antes dele é o stack.
    const rest = [...params];
    const context = typeof rest.at(-1) === "string" && rest.length > 0 ? (rest.pop() as string) : undefined;
    const stack = level === "error" || level === "fatal" ? rest.find((p): p is string => typeof p === "string") : undefined;

    const entry: Record<string, unknown> = {
      time: new Date().toISOString(),
      level: OUTPUT_LEVEL[level],
      ...this.base,
      ...(context ? { context } : {}),
      ...redact(logContext.get()),
      ...this.messageFields(message),
      ...(stack ? { stack } : {}),
    };
    this.write(safeStringify(entry));
  }

  private messageFields(message: unknown): Record<string, unknown> {
    if (message instanceof Error) {
      return { msg: message.message, errorName: message.name, stack: message.stack };
    }
    if (typeof message === "object" && message !== null) {
      // Os nossos logs estruturados: { msg: "...", campo: valor, ... }
      return redact(message as Record<string, unknown>);
    }
    return { msg: String(message) };
  }
}

function redact(fields: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(fields)) {
    out[key] = REDACTED_KEYS.has(key.toLowerCase()) ? "[redacted]" : value;
  }
  return out;
}

/** JSON.stringify que não quebra com BigInt nem com referência circular. */
function safeStringify(value: unknown): string {
  const seen = new WeakSet<object>();
  return JSON.stringify(value, (_key, v: unknown) => {
    if (typeof v === "bigint") return v.toString();
    if (typeof v === "object" && v !== null) {
      if (seen.has(v)) return "[circular]";
      seen.add(v);
    }
    return v;
  });
}
