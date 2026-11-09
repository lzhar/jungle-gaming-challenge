import { Injectable, type CanActivate } from '@nestjs/common';

/** No-op deliberado. Desenho futuro: validar JWT (OIDC) emitido por Keycloak/Zitadel e checar o escopo do provedor. */
@Injectable()
export class AuthGuard implements CanActivate {
  canActivate(): boolean {
    return true;
  }
}