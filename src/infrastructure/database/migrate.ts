/**
 * Aplica as migrations pendentes e sai: `bun src/infrastructure/database/migrate.ts`
 * (ou `bun run migrate`).
 *
 * Existe porque a CLI do MikroORM (@mikro-orm/cli) é dependência de DESENVOLVIMENTO
 * e não vai para a imagem de produção. O Migrator (@mikro-orm/migrations) vai, então
 * este script roda dentro do container: é o serviço `migrate` do docker-compose, que
 * termina antes de as instâncias da aplicação subirem.
 *
 * Idempotente: sem migrations pendentes, não faz nada. Cada migration roda numa
 * transação (allOrNothing na config): se uma falhar, nada fica pela metade e o
 * processo sai com código 1, o que impede o Compose de subir a aplicação.
 */
import { MikroORM } from "@mikro-orm/postgresql";
import config from "./mikro-orm.config.ts";

const orm = await MikroORM.init(config);
try {
  const pending = await orm.migrator.getPending();
  if (pending.length === 0) {
    console.log("migrations: nada pendente, o schema já está na última versão");
  } else {
    const applied = await orm.migrator.up();
    console.log(`migrations aplicadas: ${applied.map((migration) => migration.name).join(", ")}`);
  }
} catch (error) {
  console.error("falha ao aplicar as migrations:", error);
  process.exitCode = 1;
} finally {
  await orm.close();
}
