package io.gbmm.udps.api.auth

import cats.effect.IO
import io.gbmm.udps.integration.usp.AuthenticationContext
import org.typelevel.vault.Key

/** Vault key used to attach an authenticated user context to http4s request attributes. */
object AuthContext {
  val key: Key[AuthenticationContext] =
    Key.newKey[IO, AuthenticationContext].unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
}
