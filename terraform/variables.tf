variable "allowed_ip" {
  description = "Your public IP address to allow SSH and Airflow UI access. Get it from a site like ifconfig.me"
  type        = string
  # IMPORTANT: Replace this default with your actual IP address followed by /32
  # For example: "1.2.3.4/32"
  # Using 0.0.0.0/0 is insecure and should only be for temporary testing.
  default = "0.0.0.0/0"
}