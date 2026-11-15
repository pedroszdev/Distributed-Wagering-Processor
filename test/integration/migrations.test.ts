import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { MikroORM } from "@mikro-orm/postgresql";
import { closeTestOrm, getTestOrm, sql } from "./support/database.ts";

/**
 * Migrations "versionadas e reversíveis" (seção 4 do desafio).
 *
 * O resto da suíte já prova o `up` (o schema dos testes é criado por ele). Aqui se
 * prova o `down`, e que o ciclo up → down → up recria EXATAMENTE o mesmo schema:
 * a comparação é feita no catálogo do Postgres (tabelas, colunas, constraints,
 * índices, triggers, funções), não só "as tabelas existem".
 *
 * Ao final o schema está aplicado de novo, como os outros arquivos de teste esperam.
 */

const APP_TABLES = ["inbox_messages", "outbox_messages", "wager_transactions", "wallet_ledger_entries", "wallets"];

let orm: MikroORM;

beforeAll(async () => {
  orm = await getTestOrm();
});
afterAll(async () => {
  // Garantia: mesmo se um teste falhar no meio, o schema volta para os próximos arquivos.
  if ((await orm.migrator.getPending()).length > 0) await orm.migrator.up();
  await closeTestOrm();
});

/** Uma "foto" do schema público: tudo o que a migration cria, em texto ordenado. */
async function schemaSnapshot(): Promise<string[]> {
  const rows = await sql(`
    select 'table ' || tablename as item
      from pg_tables where schemaname = 'public' and tablename <> 'mikro_orm_migrations'
    union all
    select format('column %s.%s %s %s default=%s', table_name, column_name, data_type,
                  case when is_nullable = 'YES' then 'null' else 'not null' end, coalesce(column_default, '-'))
      from information_schema.columns
     where table_schema = 'public' and table_name <> 'mikro_orm_migrations'
    union all
    select format('constraint %s.%s %s', c.conrelid::regclass, c.conname, pg_get_constraintdef(c.oid))
      from pg_constraint c join pg_namespace n on n.oid = c.connamespace
     where n.nspname = 'public' and c.conrelid::regclass::text <> 'mikro_orm_migrations'
    union all
    select 'index ' || indexdef
      from pg_indexes where schemaname = 'public' and tablename <> 'mikro_orm_migrations'
    union all
    select format('trigger %s on %s', t.tgname, c.relname)
      from pg_trigger t join pg_class c on c.oid = t.tgrelid join pg_namespace n on n.oid = c.relnamespace
     where n.nspname = 'public' and not t.tgisinternal
    union all
    select 'function ' || p.proname
      from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'public'
    order by 1`);
  return rows.map((row) => row.item as string);
}

async function existingTables(): Promise<string[]> {
  const rows = await sql("select tablename from pg_tables where schemaname = 'public' order by tablename");
  return rows.map((row) => row.tablename as string);
}

describe("migrations: versionadas e reversíveis", () => {
  test("estão registradas na tabela de controle (versionadas) e nenhuma ficou pendente", async () => {
    const executed = await orm.migrator.getExecuted();
    expect(executed.map((m) => m.name)).toContain("Migration20261113120000_initial-schema");
    expect(await orm.migrator.getPending()).toEqual([]);
  });

  test(
    "up → down → up: o down apaga TUDO (tabelas, triggers, função) e o up recria exatamente o mesmo schema",
    async () => {
      const before = await schemaSnapshot();
      // A foto tem o que importa: as 5 tabelas, os triggers do ledger, a função e os índices parciais.
      expect(before).toEqual(expect.arrayContaining(APP_TABLES.map((t) => `table ${t}`)));
      expect(before).toContain("trigger wallet_ledger_entries_no_update_delete on wallet_ledger_entries");
      expect(before).toContain("trigger wallet_ledger_entries_no_truncate on wallet_ledger_entries");
      expect(before).toContain("function wallet_ledger_entries_immutable");
      expect(before.some((item) => item.includes("outbox_pending_due_idx") && item.includes("WHERE (published_at IS NULL)"))).toBe(true);

      // ---------------------------------------------------------------- down
      await orm.migrator.down();

      expect(await existingTables()).toEqual(["mikro_orm_migrations"]); // só a tabela de controle
      expect(await schemaSnapshot()).toEqual([]); // nada sobrou: nem trigger, nem função, nem índice
      expect(await orm.migrator.getExecuted()).toEqual([]);

      // ---------------------------------------------------------------- up de novo
      await orm.migrator.up();

      expect(await schemaSnapshot()).toEqual(before);
    },
    30_000,
  );

  test("depois do ciclo, as garantias do schema continuam valendo (ex.: o ledger segue imutável)", async () => {
    const walletId = Bun.randomUUIDv7();
    const playerId = Bun.randomUUIDv7();
    const transactionId = Bun.randomUUIDv7();
    const now = new Date();
    await sql(
      `insert into wallets (id, player_id, currency, balance_amount, version, created_at, updated_at)
       values (?, ?, 'BRL', 10.00, 1, ?, ?)`,
      [walletId, playerId, now, now],
    );
    await sql(
      `insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id,
         player_id, round_id, game_id, kind, amount, currency, status, balance_after_amount, created_at, processed_at)
       values (?, 'p', ?, ?, 'h', ?, ?, 'r', 'g', 'OPENING', 10.00, 'BRL', 'PROCESSED', 10.00, ?, ?)`,
      [transactionId, `ext-${transactionId}`, `key-${transactionId}`, walletId, playerId, now, now],
    );
    await sql(
      `insert into wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency,
         balance_before_amount, balance_after_amount, created_at)
       values (?, ?, ?, 'CREDIT', 10.00, 'BRL', 0.00, 10.00, ?)`,
      [Bun.randomUUIDv7(), walletId, transactionId, now],
    );

    await expect(sql("update wallet_ledger_entries set amount = 1 where wallet_id = ?", [walletId])).rejects.toThrow(/imutável/);
    await expect(sql("delete from wallet_ledger_entries where wallet_id = ?", [walletId])).rejects.toThrow(/imutável/);
    await expect(sql("update wallets set balance_amount = -1 where id = ?", [walletId])).rejects.toThrow(
      /wallets_balance_non_negative_ck/,
    );
  });
});
