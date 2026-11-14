import { Module } from "@nestjs/common";
import { DatabaseModule } from "./infrastructure/database/database.module.ts";
import { ProcessWagerTransactionUseCase } from "./application/wagering/process-wager-transaction.use-case.ts";
import { WageringController } from "./http/wagering.controller.ts";

@Module({
  imports: [DatabaseModule],
  providers: [ProcessWagerTransactionUseCase],
  exports: [ProcessWagerTransactionUseCase],  
  controllers: [WageringController]
})
export class WageringModule {}
