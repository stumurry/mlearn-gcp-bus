from libs.flask_app import FlaskApp
from libs.email_helper import EmailHelper
from flask_mail import Mail
from unittest.mock import patch
import os
from libs.shared.config import Config


def test_mail_config():
    sendgrid_api = "abc"
    app = FlaskApp.app()

    with patch.object(Config, 'secret', return_value=sendgrid_api) as s:
        EmailHelper.mail_config(app)
        assert app.config['SECRET_KEY'] == 'top-secret!'
        assert app.config['MAIL_SERVER'] == 'smtp.sendgrid.net'
        assert app.config['MAIL_PORT'] == 587
        assert app.config['MAIL_USE_TLS'] is True
        assert app.config['MAIL_USERNAME'] == 'apikey'
        assert app.config['MAIL_PASSWORD'] == sendgrid_api
        s.assert_called_once_with(os.environ['ENV'], 'sendgrid_api_key')
        assert app.config['MAIL_DEFAULT_SENDER'] == EmailHelper.mail_default_sender


def test_report_issue(server):
    with server.app.app_context():
        mail = server.app.mail
        assert type(mail) == Mail

        issue = 'issue'
        subject = 'subject'

        with patch.object(mail, 'send') as m:
            with patch.object(EmailHelper, 'compose_message', return_value='foo') as cm:
                EmailHelper.report_issue(issue, subject=subject)
                cm.assert_called_with(issue, subject)
                m.assert_called_once_with('foo')
