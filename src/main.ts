import "reflect-metadata";
import { ConsoleLogger, type LoggerService } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module.ts";
import { env } from "./config/env.ts";
import { JsonLogger } from "./infrastructure/observability/json-logger.ts";

/** JSON (uma linha por log) por padrão; LOG_FORMAT=pretty para ler no terminal. */
function createLogger(): LoggerService {
  return env.log.format === "pretty"
    ? new ConsoleLogger({ logLevels: levelsFrom(env.log.level) })
    : new JsonLogger(env.log.level);
}

function levelsFrom(min: typeof env.log.level) {
  const all = ["verbose", "debug", "log", "warn", "error", "fatal"] as const;
  return all.slice(all.indexOf(min));
}

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule, { logger: createLogger() });

  // SIGTERM → o Nest chama beforeApplicationShutdown/onApplicationShutdown: os
  // workers e o consumidor terminam o que está em andamento antes de sair.
  app.enableShutdownHooks();

  await app.listen(env.port);
}

await bootstrap();
