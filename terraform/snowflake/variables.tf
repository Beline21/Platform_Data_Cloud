variable "snowflake_organization" {
  description = "Snowflake organization name (ex: myorg)"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account identifier (ex: xy12345.eu-central-1)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
  default     = "svc_terraform"
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}
