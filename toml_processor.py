import toml

config = None

def load_config() -> dict:
    global config
    if config is None:
        with open('config.toml', mode='r', encoding='utf-8') as file:
            config = toml.load(file)
    return config

