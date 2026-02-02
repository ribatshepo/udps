package io.gbmm.udps.governance.classification

import scala.util.matching.Regex

/** Enumeration of recognized PII categories. */
sealed trait PIICategory {
  def name: String
  def riskLevel: RiskLevel
}

/** Risk levels for PII categories used in classification prioritization. */
sealed trait RiskLevel extends Ordered[RiskLevel] {
  def weight: Int
  override def compare(that: RiskLevel): Int = this.weight - that.weight
}

object RiskLevel {
  case object Critical extends RiskLevel { val weight = 4 }
  case object High extends RiskLevel { val weight = 3 }
  case object Medium extends RiskLevel { val weight = 2 }
  case object Low extends RiskLevel { val weight = 1 }
}

object PIICategory {
  case object SSN extends PIICategory {
    val name = "SSN"
    val riskLevel: RiskLevel = RiskLevel.Critical
  }
  case object CreditCard extends PIICategory {
    val name = "CreditCard"
    val riskLevel: RiskLevel = RiskLevel.Critical
  }
  case object Email extends PIICategory {
    val name = "Email"
    val riskLevel: RiskLevel = RiskLevel.Medium
  }
  case object Phone extends PIICategory {
    val name = "Phone"
    val riskLevel: RiskLevel = RiskLevel.Medium
  }
  case object IPAddress extends PIICategory {
    val name = "IPAddress"
    val riskLevel: RiskLevel = RiskLevel.Low
  }
  case object Passport extends PIICategory {
    val name = "Passport"
    val riskLevel: RiskLevel = RiskLevel.Critical
  }
  case object DriverLicense extends PIICategory {
    val name = "DriverLicense"
    val riskLevel: RiskLevel = RiskLevel.High
  }
  case object DateOfBirth extends PIICategory {
    val name = "DateOfBirth"
    val riskLevel: RiskLevel = RiskLevel.High
  }
  case object HomeAddress extends PIICategory {
    val name = "HomeAddress"
    val riskLevel: RiskLevel = RiskLevel.High
  }
  case object BiometricData extends PIICategory {
    val name = "BiometricData"
    val riskLevel: RiskLevel = RiskLevel.Critical
  }
  case object FinancialAccount extends PIICategory {
    val name = "FinancialAccount"
    val riskLevel: RiskLevel = RiskLevel.Critical
  }
  case object MedicalRecord extends PIICategory {
    val name = "MedicalRecord"
    val riskLevel: RiskLevel = RiskLevel.Critical
  }
  case object Username extends PIICategory {
    val name = "Username"
    val riskLevel: RiskLevel = RiskLevel.Low
  }
  case object Password extends PIICategory {
    val name = "Password"
    val riskLevel: RiskLevel = RiskLevel.Critical
  }
  case object APIKey extends PIICategory {
    val name = "APIKey"
    val riskLevel: RiskLevel = RiskLevel.Critical
  }
  case object JWTToken extends PIICategory {
    val name = "JWTToken"
    val riskLevel: RiskLevel = RiskLevel.Critical
  }

  /** All built-in PII categories. */
  val all: Seq[PIICategory] = Seq(
    SSN, CreditCard, Email, Phone, IPAddress, Passport,
    DriverLicense, DateOfBirth, HomeAddress, BiometricData,
    FinancialAccount, MedicalRecord, Username, Password,
    APIKey, JWTToken
  )

  /** Resolve a category by name (case-insensitive). */
  def fromName(name: String): Option[PIICategory] =
    all.find(_.name.equalsIgnoreCase(name))
}

/**
 * A compiled PII detection pattern with optional validator.
 *
 * @param category    the PII category this pattern detects
 * @param regex       compiled regular expression for content matching
 * @param description human-readable description of the pattern
 * @param validator   optional secondary validation function (e.g. Luhn check)
 */
final case class PIIPattern(
    category: PIICategory,
    regex: Regex,
    description: String,
    validator: Option[String => Boolean]
)

/** Built-in PII detection patterns with regex and optional validators. */
object BuiltInPatterns {

  private val luhnDigitBase = 10

  /** Validates a number string using the Luhn algorithm. */
  def luhnCheck(raw: String): Boolean = {
    val digits = raw.filter(_.isDigit)
    if (digits.length < 2) return false
    val sum = digits.reverse.zipWithIndex.foldLeft(0) { case (acc, (ch, idx)) =>
      val d = ch.asDigit
      if (idx % 2 == 1) {
        val doubled = d * 2
        acc + (if (doubled >= luhnDigitBase) doubled - (luhnDigitBase - 1) else doubled)
      } else {
        acc + d
      }
    }
    sum % luhnDigitBase == 0
  }

  /** All built-in PII patterns for content scanning. */
  val all: Seq[PIIPattern] = Seq(
    PIIPattern(
      PIICategory.SSN,
      """(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)""".r,
      "US Social Security Number (XXX-XX-XXXX)",
      Some((s: String) => {
        val digits = s.filter(_.isDigit)
        digits.length == 9 && !digits.startsWith("000") &&
          !digits.startsWith("666") && !digits.startsWith("9")
      })
    ),
    PIIPattern(
      PIICategory.CreditCard,
      """(?<!\d)\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}(?!\d)""".r,
      "Credit/debit card number (16 digits with optional separators)",
      Some(luhnCheck)
    ),
    PIIPattern(
      PIICategory.Email,
      """(?i)[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}""".r,
      "Email address",
      None
    ),
    PIIPattern(
      PIICategory.Phone,
      """(?<!\d)(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}(?!\d)""".r,
      "US/international phone number",
      Some((s: String) => {
        val digits = s.filter(_.isDigit)
        digits.length >= 10 && digits.length <= 15
      })
    ),
    PIIPattern(
      PIICategory.IPAddress,
      """(?<!\d)(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)(?!\d)""".r,
      "IPv4 address",
      None
    ),
    PIIPattern(
      PIICategory.Passport,
      """(?i)(?<![A-Z0-9])[A-Z]{1,2}\d{6,9}(?![A-Z0-9])""".r,
      "Passport number (letter prefix + 6-9 digits)",
      Some((s: String) => {
        val cleaned = s.trim.toUpperCase
        cleaned.length >= 7 && cleaned.length <= 11
      })
    ),
    PIIPattern(
      PIICategory.DriverLicense,
      """(?i)(?<![A-Z0-9])[A-Z]\d{3,8}(?:[-\s]?\d{3,8})?(?![A-Z0-9])""".r,
      "Driver license number (letter + digits, varies by state)",
      None
    ),
    PIIPattern(
      PIICategory.DateOfBirth,
      """(?<!\d)(?:0[1-9]|1[0-2])[/\-](?:0[1-9]|[12]\d|3[01])[/\-](?:19|20)\d{2}(?!\d)""".r,
      "Date of birth (MM/DD/YYYY or MM-DD-YYYY)",
      None
    ),
    PIIPattern(
      PIICategory.HomeAddress,
      """(?i)\d+\s+[a-z]+(?:\s+[a-z]+)*\s+(?:st|street|ave|avenue|blvd|boulevard|dr|drive|rd|road|ln|lane|ct|court|way|pl|place)\.?""".r,
      "Street address pattern",
      None
    ),
    PIIPattern(
      PIICategory.BiometricData,
      """(?i)(?:fingerprint|retina|iris|face[_\s]?id|voice[_\s]?print|biometric)[:\s_]""".r,
      "Biometric data identifier or label",
      None
    ),
    PIIPattern(
      PIICategory.FinancialAccount,
      """(?<!\d)\d{8,17}(?!\d)""".r,
      "Bank account or routing number (8-17 digits)",
      Some((s: String) => {
        val digits = s.filter(_.isDigit)
        digits.length >= 8 && digits.length <= 17
      })
    ),
    PIIPattern(
      PIICategory.MedicalRecord,
      """(?i)(?:mrn|medical[_\s]?record|patient[_\s]?id)[:\s_#]?\s*\w+""".r,
      "Medical record number or patient identifier",
      None
    ),
    PIIPattern(
      PIICategory.Username,
      """(?i)(?:user[_\s]?name|login[_\s]?id|user[_\s]?id)[:\s=]+\S+""".r,
      "Username or login identifier",
      None
    ),
    PIIPattern(
      PIICategory.Password,
      """(?i)(?:password|passwd|pwd|secret)[:\s=]+\S+""".r,
      "Password or secret value in text",
      None
    ),
    PIIPattern(
      PIICategory.APIKey,
      """(?i)(?:api[_\s]?key|apikey|access[_\s]?key)[:\s=]+\S+""".r,
      "API key or access key in text",
      None
    ),
    PIIPattern(
      PIICategory.JWTToken,
      """eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+""".r,
      "JSON Web Token (JWT)",
      Some((s: String) => {
        val parts = s.split('.')
        parts.length == 3 && parts.forall(_.nonEmpty)
      })
    )
  )

  /** Column name heuristics: maps lowercase column name substrings to PII categories. */
  val columnNameHeuristics: Map[String, PIICategory] = Map(
    "ssn" -> PIICategory.SSN,
    "social_security" -> PIICategory.SSN,
    "socialsecurity" -> PIICategory.SSN,
    "credit_card" -> PIICategory.CreditCard,
    "creditcard" -> PIICategory.CreditCard,
    "card_number" -> PIICategory.CreditCard,
    "cardnumber" -> PIICategory.CreditCard,
    "cc_number" -> PIICategory.CreditCard,
    "ccnumber" -> PIICategory.CreditCard,
    "email" -> PIICategory.Email,
    "email_address" -> PIICategory.Email,
    "emailaddress" -> PIICategory.Email,
    "phone" -> PIICategory.Phone,
    "phone_number" -> PIICategory.Phone,
    "phonenumber" -> PIICategory.Phone,
    "telephone" -> PIICategory.Phone,
    "mobile" -> PIICategory.Phone,
    "cell_phone" -> PIICategory.Phone,
    "ip_address" -> PIICategory.IPAddress,
    "ipaddress" -> PIICategory.IPAddress,
    "ip_addr" -> PIICategory.IPAddress,
    "client_ip" -> PIICategory.IPAddress,
    "passport" -> PIICategory.Passport,
    "passport_number" -> PIICategory.Passport,
    "passportnumber" -> PIICategory.Passport,
    "driver_license" -> PIICategory.DriverLicense,
    "driverlicense" -> PIICategory.DriverLicense,
    "license_number" -> PIICategory.DriverLicense,
    "dl_number" -> PIICategory.DriverLicense,
    "date_of_birth" -> PIICategory.DateOfBirth,
    "dateofbirth" -> PIICategory.DateOfBirth,
    "dob" -> PIICategory.DateOfBirth,
    "birth_date" -> PIICategory.DateOfBirth,
    "birthdate" -> PIICategory.DateOfBirth,
    "address" -> PIICategory.HomeAddress,
    "home_address" -> PIICategory.HomeAddress,
    "street_address" -> PIICategory.HomeAddress,
    "mailing_address" -> PIICategory.HomeAddress,
    "biometric" -> PIICategory.BiometricData,
    "fingerprint" -> PIICategory.BiometricData,
    "face_id" -> PIICategory.BiometricData,
    "bank_account" -> PIICategory.FinancialAccount,
    "account_number" -> PIICategory.FinancialAccount,
    "routing_number" -> PIICategory.FinancialAccount,
    "iban" -> PIICategory.FinancialAccount,
    "medical_record" -> PIICategory.MedicalRecord,
    "mrn" -> PIICategory.MedicalRecord,
    "patient_id" -> PIICategory.MedicalRecord,
    "username" -> PIICategory.Username,
    "user_name" -> PIICategory.Username,
    "login" -> PIICategory.Username,
    "login_id" -> PIICategory.Username,
    "password" -> PIICategory.Password,
    "passwd" -> PIICategory.Password,
    "pwd" -> PIICategory.Password,
    "api_key" -> PIICategory.APIKey,
    "apikey" -> PIICategory.APIKey,
    "access_key" -> PIICategory.APIKey,
    "secret_key" -> PIICategory.APIKey,
    "jwt" -> PIICategory.JWTToken,
    "jwt_token" -> PIICategory.JWTToken,
    "auth_token" -> PIICategory.JWTToken,
    "bearer_token" -> PIICategory.JWTToken
  )
}
