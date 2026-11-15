import { Injectable } from "@nestjs/common";

/** Quem está chamando a API: um provedor de jogos autenticado. */
export interface ProviderPrincipal {
  providerId: string;
}

/** O que o guard precisa da requisição para autenticar (sem acoplar ao Express). */
export interface AuthenticationRequest {
  headers: Record<string, string | string[] | undefined>;
  method: string;
  path: string;
}

export type AuthenticationResult =
  | { authenticated: true; principal?: ProviderPrincipal }
  | { authenticated: false; reason: string };

/**
 * PORTA de autenticação (seção 2 do desafio: "deixe o ponto de extensão explícito").
 *
 * Hoje a implementação é a NoopProviderAuthenticator (deixa tudo passar, sem
 * principal). Para integrar um Identity Provider de verdade (Keycloak/Zitadel,
 * OIDC), basta outra implementação desta classe, por exemplo:
 *
 *   class OidcProviderAuthenticator extends ProviderAuthenticator {
 *     async authenticate({ headers }) {
 *       // 1. lê "Authorization: Bearer <jwt>"
 *       // 2. valida assinatura (JWKS do IdP), issuer, audience e expiração
 *       // 3. client credentials: o client_id / claim "provider_id" vira o principal
 *       return { authenticated: true, principal: { providerId: claims.provider_id } };
 *     }
 *   }
 *
 * e trocar o `useClass` no AppModule. Guard, controllers e regras não mudam.
 */
export abstract class ProviderAuthenticator {
  abstract authenticate(request: AuthenticationRequest): Promise<AuthenticationResult>;
}

/**
 * Autenticação desligada (decisão documentada no ARCHITECTURE.md): toda requisição
 * passa e NÃO há principal, então a checagem "providerId do corpo = provedor
 * autenticado" também fica inativa. Nunca usar assim exposto à internet.
 */
@Injectable()
export class NoopProviderAuthenticator extends ProviderAuthenticator {
  override async authenticate(): Promise<AuthenticationResult> {
    return { authenticated: true };
  }
}
