package io.gbmm.udps.storage.tiering

import cats.effect.{IO, Resource}
import com.typesafe.scalalogging.LazyLogging
import io.gbmm.udps.core.config.{MinioConfig, StorageConfig}
import io.gbmm.udps.core.domain.StorageTier

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.Instant

/** Result of a tier transition operation. */
final case class TierTransitionResult(
  filePath: String,
  previousTier: StorageTier,
  newTier: StorageTier,
  transitionedAt: Instant
)

/** Metadata update emitted after a tier change for external persistence. */
final case class TierMetadataUpdate(
  filePath: String,
  currentTier: StorageTier,
  physicalLocation: String,
  updatedAt: Instant
)

/**
 * Manages data across four storage tiers:
 *   Hot  (local NVMe) -> Warm (local SSD) -> Cold (local HDD) -> Archive (MinIO S3)
 *
 * New data lands in Hot by default. The policy engine evaluates files for
 * demotion or promotion. Reads are transparent -- if a file is not in the
 * expected tier the manager fetches it automatically.
 */
final class TierManager private (
  storageConfig: StorageConfig,
  minioClient: MinIOClient,
  policy: TierPolicy
) extends LazyLogging {

  private val hotBase: Path  = Paths.get(storageConfig.hotPath)
  private val warmBase: Path = Paths.get(storageConfig.warmPath)
  private val coldBase: Path = Paths.get(storageConfig.coldPath)

  // ---- Write path ----

  /** Write new data to the Hot tier. Returns the physical path written. */
  def writeToHot(relativePath: String, data: Array[Byte]): IO[TierMetadataUpdate] =
    IO.blocking {
      val target = hotBase.resolve(relativePath)
      Option(target.getParent).foreach(parent => Files.createDirectories(parent))
      Files.write(target, data)
      logger.info("Wrote {} bytes to Hot tier: {}", data.length, target)
      TierMetadataUpdate(
        filePath = relativePath,
        currentTier = StorageTier.Hot,
        physicalLocation = target.toString,
        updatedAt = Instant.now()
      )
    }

  // ---- Tier evaluation ----

  /** Evaluate the tier policy for a file and return the recommended tier.
   *  Does NOT perform the transition.
   */
  def evaluateTier(info: DataFileInfo): IO[StorageTier] =
    IO.delay(policy.evaluate(info, Instant.now()))

  /** Evaluate and, if needed, transition a file to its recommended tier.
   *  Returns `None` if the file is already in the correct tier.
   */
  def evaluateAndTransition(info: DataFileInfo): IO[Option[TierTransitionResult]] =
    evaluateTier(info).flatMap { targetTier =>
      if (targetTier == info.currentTier) IO.pure(None)
      else transition(info.filePath, info.currentTier, targetTier).map(Some(_))
    }

  // ---- Tier transition ----

  /** Transition a file from one tier to another. */
  def transition(
    relativePath: String,
    sourceTier: StorageTier,
    targetTier: StorageTier
  ): IO[TierTransitionResult] =
    if (sourceTier == targetTier)
      IO.raiseError(new IllegalArgumentException(
        s"Source and target tier are the same: $sourceTier"
      ))
    else
      performTransition(relativePath, sourceTier, targetTier).map { _ =>
        logger.info("Transitioned {} from {} to {}", relativePath, sourceTier, targetTier)
        TierTransitionResult(
          filePath = relativePath,
          previousTier = sourceTier,
          newTier = targetTier,
          transitionedAt = Instant.now()
        )
      }

  // ---- Transparent reads ----

  /** Read a file, fetching it into Hot tier if not locally available. */
  def readFile(relativePath: String, currentTier: StorageTier): IO[Path] =
    currentTier match {
      case StorageTier.Archive => fetchFromArchiveToHot(relativePath)
      case tier                => resolveLocalPath(relativePath, tier)
    }

  /** Read bytes from a file in any tier. */
  def readBytes(relativePath: String, currentTier: StorageTier): IO[Array[Byte]] =
    readFile(relativePath, currentTier).flatMap { path =>
      IO.blocking(Files.readAllBytes(path))
    }

  // ---- Metadata helpers ----

  /** Build a metadata update record for a file after transition. */
  def metadataUpdate(relativePath: String, tier: StorageTier): IO[TierMetadataUpdate] =
    IO.delay {
      val location = tier match {
        case StorageTier.Archive => s"s3://${relativePath}"
        case _                   => tierBasePath(tier).resolve(relativePath).toString
      }
      TierMetadataUpdate(
        filePath = relativePath,
        currentTier = tier,
        physicalLocation = location,
        updatedAt = Instant.now()
      )
    }

  /** Check if a file exists in a given tier. */
  def existsInTier(relativePath: String, tier: StorageTier): IO[Boolean] =
    tier match {
      case StorageTier.Archive => minioClient.exists(relativePath)
      case _                   => IO.blocking(Files.exists(tierBasePath(tier).resolve(relativePath)))
    }

  // ---- Internal ----

  private def performTransition(
    relativePath: String,
    source: StorageTier,
    target: StorageTier
  ): IO[Unit] =
    (source, target) match {
      case (StorageTier.Archive, t) =>
        downloadFromArchive(relativePath, t)
      case (s, StorageTier.Archive) =>
        uploadToArchive(relativePath, s)
      case (_, _) =>
        localToLocal(relativePath, source, target)
    }

  private def localToLocal(relativePath: String, source: StorageTier, target: StorageTier): IO[Unit] =
    IO.blocking {
      val sourcePath = tierBasePath(source).resolve(relativePath)
      val targetPath = tierBasePath(target).resolve(relativePath)
      Option(targetPath.getParent).foreach(parent => Files.createDirectories(parent))
      Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING)
      Files.deleteIfExists(sourcePath)
      ()
    }

  private def uploadToArchive(relativePath: String, source: StorageTier): IO[Unit] = {
    val sourcePath = tierBasePath(source).resolve(relativePath)
    minioClient.upload(sourcePath, relativePath) *>
      IO.blocking { Files.deleteIfExists(sourcePath); () }
  }

  private def downloadFromArchive(relativePath: String, target: StorageTier): IO[Unit] = {
    val targetPath = tierBasePath(target).resolve(relativePath)
    minioClient.download(relativePath, targetPath) *>
      minioClient.delete(relativePath)
  }

  private def fetchFromArchiveToHot(relativePath: String): IO[Path] = {
    val hotPath = hotBase.resolve(relativePath)
    IO.blocking(Files.exists(hotPath)).flatMap {
      case true  => IO.pure(hotPath)
      case false =>
        IO.blocking {
          Option(hotPath.getParent).foreach(parent => Files.createDirectories(parent))
        } *> minioClient.download(relativePath, hotPath).as(hotPath)
    }
  }

  private def resolveLocalPath(relativePath: String, tier: StorageTier): IO[Path] = {
    val path = tierBasePath(tier).resolve(relativePath)
    IO.blocking(Files.exists(path)).flatMap {
      case true  => IO.pure(path)
      case false =>
        IO.raiseError(new java.io.FileNotFoundException(
          s"File not found in $tier tier: $path"
        ))
    }
  }

  private def tierBasePath(tier: StorageTier): Path = tier match {
    case StorageTier.Hot     => hotBase
    case StorageTier.Warm    => warmBase
    case StorageTier.Cold    => coldBase
    case StorageTier.Archive =>
      throw new IllegalArgumentException("Archive tier has no local base path")
  }
}

object TierManager {

  /** Build a TierManager as a cats-effect Resource. */
  def resource(
    storageConfig: StorageConfig,
    minioConfig: MinioConfig,
    policy: TierPolicy
  ): Resource[IO, TierManager] =
    for {
      _      <- Resource.eval(ensureDirectories(storageConfig))
      client <- MinIOClient.resource(minioConfig)
    } yield new TierManager(storageConfig, client, policy)

  /** Convenience overload using the default tier policy. */
  def resource(
    storageConfig: StorageConfig,
    minioConfig: MinioConfig
  ): Resource[IO, TierManager] =
    resource(storageConfig, minioConfig, TierPolicy.default)

  private def ensureDirectories(config: StorageConfig): IO[Unit] =
    IO.blocking {
      val _ = Files.createDirectories(Paths.get(config.hotPath))
      val _ = Files.createDirectories(Paths.get(config.warmPath))
      val _ = Files.createDirectories(Paths.get(config.coldPath))
    }
}
