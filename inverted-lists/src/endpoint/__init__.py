from flask import Flask, render_template
from random import randint
import time

from src.endpoint.blueprints.home import home

# To launch from shell: FLASK_APP=src.endpoint FLASK_DEBUG=on flask run


def create_app(environment_name='dev'):
    app = Flask(__name__)

    # custom config
    # app.config.from_object(configs[environment_name])

    @app.errorhandler(500)
    def handle_error(exception):
        return render_template('500.html')  # pragma: no cover

    @app.template_filter()
    def format_datetime(value, format='medium'):
        if format == 'full':
            format = "EEEE, d. MMMM y 'at' HH:mm"
        elif format == 'medium':
            format = "EE dd.MM.y HH:mm"
        # return babel.dates.format_datetime(value, format)
        # return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime(value))
        return time.strftime("%d %b %Y at %H:%M", time.gmtime(value))
        # return time.ctime(value)

    app.register_blueprint(home, url_prefix='/')
    return app
