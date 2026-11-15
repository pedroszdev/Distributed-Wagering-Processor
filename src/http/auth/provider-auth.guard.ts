import { createParamDecorator, Injectable, SetMetadata, type CanActivate, type ExecutionContext } from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import type { Request } from "express";
import { ProviderMismatchError, UnauthenticatedError } from "./auth.errors.ts";
import { ProviderAuthenticator, type ProviderPrincipal } from "./provider-authenticator.ts";

const IS_PUBLIC = "auth:isPublic";

/** Rota aberta, sem autenticação (health checks e métricas, seção 2). */
export const Public = () => SetMetadata(IS_PUBLIC, true);

type AuthenticatedRequest = Request & { provider?: ProviderPrincipal };

/**
 * Guard GLOBAL: toda rota passa por aqui, menos as marcadas com @Public().
 * Quem decide "quem é" é a porta ProviderAuthenticator; o guard só aplica.
 *
 * Mensagens da fila não passam por aqui: são canal interno confiável (seção 2),
 * mas o providerId delas continua sujeito às mesmas regras de domínio.
 */
@Injectable()
export class ProviderAuthGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly authenticator: ProviderAuthenticator,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC, [context.getHandler(), context.getClass()]);
    if (isPublic) return true;

    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const result = await this.authenticator.authenticate({
      headers: request.headers,
      method: request.method,
      path: request.path,
    });
    if (!result.authenticated) throw new UnauthenticatedError(result.reason);

    request.provider = result.principal;
    return true;
  }
}

/** O provedor autenticado desta requisição (undefined com o NoopProviderAuthenticator). */
export const CurrentProvider = createParamDecorator(
  (_data: unknown, context: ExecutionContext): ProviderPrincipal | undefined =>
    context.switchToHttp().getRequest<AuthenticatedRequest>().provider,
);

/**
 * Um provedor só opera em nome de si mesmo: o providerId do corpo/rota precisa
 * ser o do provedor autenticado. Sem principal (auth desligada), não há o que checar.
 */
export function assertActsAs(principal: ProviderPrincipal | undefined, providerId: string): void {
  if (principal && principal.providerId !== providerId) {
    throw new ProviderMismatchError(principal.providerId, providerId);
  }
}
