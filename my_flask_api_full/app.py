from dotenv import load_dotenv
load_dotenv()  # must be first

from flask import Flask
from routes.plan_routes import bp as plan_routes
from services.elasticsearch_service import ensure_index

def create_app():
    app = Flask(__name__)
    ensure_index() 

    # Register Blueprints
    app.register_blueprint(plan_routes, url_prefix='/api/v1')

    # Health check endpoint (useful for Docker healthchecks)
    @app.route('/health', methods=['GET'])
    def health():
        return {"status": "ok"}, 200

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(
        debug=True,               # remove or set via env in production
        port=3000,
        host="0.0.0.0"
    )
