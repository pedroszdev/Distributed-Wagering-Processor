import { Module } from "@nestjs/common";
import { DatabaseModule } from "./infrastructure/database/database.module.ts";
import { ProcessWagerTransactionUseCase } from "./application/wagering/process-wager-transaction.use-case.ts";
import { WageringController } from "./http/wagering.controller.ts";
import { GetWagerTransactionUseCase } from "./application/wagering/get-wager-transaction.use-case.ts";
import { WagerRules } from "./application/wagering/wager-rules.ts";
import { ResolvePendingReferencesUseCase } from "./application/wagering/resolve-pending-references.use-case.ts";
import { PENDING_REFERENCE_POLICY, type PendingReferencePolicy } from "./application/wagering/pending-reference-policy.ts";
import { PendingReferenceWorker } from "./infrastructure/scheduling/pending-reference.worker.ts";
import { env } from "./config/env.ts";

const pendingReferencePolicy: PendingReferencePolicy = {
  ttlMs: env.pendingReference.ttlMs,
  baseDelayMs: env.pendingReference.retryBaseDelayMs,
  maxDelayMs: env.pendingReference.retryMaxDelayMs,
};

@Module({
  imports: [DatabaseModule],
  providers: [
    WagerRules,
    ProcessWagerTransactionUseCase,
    GetWagerTransactionUseCase,
    ResolvePendingReferencesUseCase,
    PendingReferenceWorker,
    { provide: PENDING_REFERENCE_POLICY, useValue: pendingReferencePolicy },
  ],
  exports: [ProcessWagerTransactionUseCase, ResolvePendingReferencesUseCase],
  controllers: [WageringController],
})
export class WageringModule {}
