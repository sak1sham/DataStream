from typing import Dict
from config.settings import settings

import importlib.util

def get_mapping(id: str) -> Dict:
    f_name = "config/jobs/" + id + ".py"
    mod_name = id + "./py"
    spec = importlib.util.spec_from_file_location(mod_name, f_name)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    return foo.mapping    
