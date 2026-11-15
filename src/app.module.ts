import { Module, type MiddlewareConsumer, type NestModule } from "@nestjs/common";
import { APP_FILTER } from "@nestjs/core";
import { MikroOrmModule } from "@mikro-orm/nestjs";
import { DomainErrorFilter } from "./http/domain-error.filter.ts";
import { RequestContextMiddleware } from "./http/request-context.middleware.ts";
import ormConfig from "./infrastructure/database/mikro-orm.config.ts";
import { WalletsModule } from "./wallets.module.ts";
import { WageringModule } from "./wagering.module.ts";
import { OutboxModule } from "./outbox.module.ts";
import { WagerConsumerModule } from "./wager-consumer.module.ts";
import { ObservabilityModule } from "./observability.module.ts";

@Module({
  imports: [
    MikroOrmModule.forRoot(ormConfig),
    ObservabilityModule, // @Global: Metrics disponível para todos
    WalletsModule,
    WageringModule,
    OutboxModule,
    WagerConsumerModule,
  ],
  // Filter global: vale para todos os controllers.
  providers: [{ provide: APP_FILTER, useClass: DomainErrorFilter }],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    // Toda rota: correlationId, contexto de log, duração e access log.
    consumer.apply(RequestContextMiddleware).forRoutes("*");
  }
}
