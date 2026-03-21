from src.alerts.email_alert import EmailAlert

email = EmailAlert(
    "smtp.office365.com",
    587,
    "pradeep_kumar9@comcast.com",
    "Pradeepkumar@1995",
    ["pradeep_kumar9@comcast.com"]
)

email.send_failure_summary(4, 2, 2)