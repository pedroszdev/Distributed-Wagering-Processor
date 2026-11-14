import { Module } from "@nestjs/common";
import { DatabaseModule } from "./infrastructure/database/database.module.ts";
import { ProcessWagerTransactionUseCase } from "./application/wagering/process-wager-transaction.use-case.ts";
import { WageringController } from "./http/wagering.controller.ts";
import { GetWagerTransactionUseCase } from "./application/wagering/get-wager-transaction.use-case.ts";

@Module({
  imports: [DatabaseModule],
  providers: [ProcessWagerTransactionUseCase, GetWagerTransactionUseCase],
  exports: [ProcessWagerTransactionUseCase],  
  controllers: [WageringController]
})
export class WageringModule {}
