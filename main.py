
from app.app import app, load_config
from upload_call_history import add_call_history_to_datastore
from import_call_history import import_call_history_data
from models.config import Config


def upload_call_history(_event, _context):
    config = Config.from_env()
    config.log()

    merged_call_history = import_call_history_data(config)

    status = add_call_history_to_datastore(merged_call_history)
    print(status)

    return status


load_config(app)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5011)
