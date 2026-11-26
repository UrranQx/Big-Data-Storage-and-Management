"""Create the local Superset administrator on first initialization."""

import os

from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import appbuilder

    manager = appbuilder.sm
    if manager.find_user(username="admin") is None:
        manager.add_user(
            username="admin",
            first_name="Lab",
            last_name="Administrator",
            email="admin@localhost",
            role=manager.find_role("Admin"),
            password=os.environ["SUPERSET_ADMIN_PASSWORD"],
        )
    print("Superset administrator ready")
