from libs.flask_app import FlaskApp


def test_app(server):
    app = FlaskApp.app()
    assert app is not None
    assert app == server.app
