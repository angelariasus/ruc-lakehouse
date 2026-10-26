variable "project" {
  description = "Name prefix for every resource."
  type        = string
  default     = "ruc-lakehouse"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project))
    error_message = "Use 3 to 31 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "us-east-1"
}

variable "alert_email" {
  description = "Address that receives the budget alerts."
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "Must be an email address."
  }
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget for the account, in US dollars."
  type        = number
  default     = 15
}

variable "landing_archive_days" {
  description = "Days before landed zips move to Glacier Instant Retrieval. They are never deleted: SUNAT keeps no history."
  type        = number
  default     = 30
}

variable "staging_expiry_days" {
  description = "Days to keep the re-chunked staging files, which can be rebuilt from landing."
  type        = number
  default     = 7
}

variable "landing_version_days" {
  description = "Days a landed zip stays recoverable after being deleted or overwritten."
  type        = number
  default     = 30
}

variable "query_scan_limit_gb" {
  description = "Athena cancels any query that scans more than this many GiB."
  type        = number
  default     = 10
}

variable "emr_release_label" {
  description = "EMR Serverless release: Spark 4.1 runtime, matching the local PySpark version."
  type        = string
  default     = "emr-spark-8.1.0"
}

variable "emr_architecture" {
  description = "CPU architecture of the EMR Serverless workers."
  type        = string
  default     = "X86_64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.emr_architecture)
    error_message = "Must be X86_64 or ARM64."
  }
}

variable "emr_max_vcpu" {
  description = "Upper limit of vCPUs the Spark application may use at once."
  type        = number
  default     = 16
}

variable "emr_max_memory_gb" {
  description = "Upper limit of memory, in GB, the Spark application may use at once."
  type        = number
  default     = 64
}

variable "job_image_tag" {
  description = "Tag of the job image in ECR. Leave null until the first image is pushed; the application then uses the stock EMR image."
  type        = string
  default     = null
}

variable "s3_use_path_style" {
  description = "Path-style S3 requests, for emulators such as Moto. Keep false on AWS."
  type        = bool
  default     = false
}
