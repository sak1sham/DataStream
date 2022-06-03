from typing import Dict, Any
import importlib.util

def get_mapping(id: str) -> Dict:
    f_name = f"config/jobs/{id}.py"
    mod_name = f"{id}./py"
    spec = importlib.util.spec_from_file_location(mod_name, f_name)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    return foo.mapping    


def get_kafka_mapping_functions(id: str) -> Any:
    f_name = f"config/jobs/{id}.py"
    mod_name = f"{id}./py"
    spec = importlib.util.spec_from_file_location(mod_name, f_name)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    return foo.get_table_name, foo.process_dict    
