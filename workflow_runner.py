from typing import Dict, Any, List
import json

class WorkflowExecutor:
    def __init__(self, workflow_data: Dict[str, Any]):
        self.nodes = {node['id']: node for node in workflow_data.get('nodes', [])}
        self.edges = workflow_data.get('edges', [])
        # Map output node ID -> List of edge dicts {target, handle}
        self.adjacency = {}
        for edge in self.edges:
            source = edge['source']
            target = edge['target']
            handle = edge.get('sourceHandle') # 'true' or 'false' for logic nodes. Null for others.
            
            if source not in self.adjacency:
                self.adjacency[source] = []
            self.adjacency[source].append({'target': target, 'handle': handle})
            
        self.context = {} # Variable storage
        self.execution_log = []

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the workflow starting from the 'api' node.
        """
        # 1. Initialize all Variables first (Global Scope)
        for node_id, node in self.nodes.items():
            if node['type'] == 'variable':
                self.execute_node(node)
                
        # Find start node (API Node)
        start_node = None
        for node_id, node in self.nodes.items():
            if node['type'] == 'api':
                start_node = node
                break
        
        if not start_node:
            print("No API Entry found")
            return {"error": "No API Entry Point found"}

        self.execution_log.append(f"Started execution at {start_node['data'].get('label', 'API Entry')}")
        
        # Initialize context with request data flattened for easier access
        # input_data structure from invoke.py: { "method": ..., "body": {}, "query": {}, "params": {} }
        self.context['request'] = input_data
        
        # Expose top-level convenience variables for non-tech users
        # Users can just use {body}, {query.id}, {params.userId} etc.
        if isinstance(input_data.get('body'), dict):
            self.context['body'] = input_data['body']
        if input_data.get('query'):
            self.context['query'] = input_data['query']
        if input_data.get('params'):
            self.context['params'] = input_data['params']
        
        # Traverse
        current_nodes = [start_node['id']]
        
        visited = set()
        response = None

        # Max iterations to prevent infinite loops (naive check)
        entry_count = 0 
        
        while current_nodes and entry_count < 1000:
            entry_count += 1
            next_layer = []
            
            # Process current layer
            for node_id in current_nodes:
                # Allow re-visiting for loops, but for DAGs we might want visited check.
                # For this simple implementation, we allow re-visit if it's a different path, 
                # but 'visited' set prevents infinite cycles for now if we strictly track ID.
                # Removing strict visited check to allow merging paths, but we need loop detection.
                # For now: simple visited check per run? No, visited check kills merge paths.
                # Let's just run. Set-based 'visited' is bad for merge nodes (multiple inputs).
                # We simply consume the current wave.
                
                node = self.nodes[node_id]
                node_type = node['type']
                self.execution_log.append(f"Executing Node: {node_type} ({node_id})")

                try:
                    res = self.execute_node(node)
                    
                    # Check for immediate response
                    if res and res.get('type') == 'response':
                        response = res.get('data')
                        return {
                            "status": "success",
                            "response": response,
                            "logs": self.execution_log,
                            "context": self.context
                        }
                    
                    # Handle Logic/Loop Outcome
                    node_result = None
                    if res and res.get('type') in ['logic', 'loop']:
                        node_result = res.get('result')

                except Exception as e:
                    self.execution_log.append(f"Error executing node {node_id}: {str(e)}")
                    return {
                        "status": "error",
                        "error": str(e),
                        "logs": self.execution_log
                    }

                # Add downstream nodes with routing logic
                if node_id in self.adjacency:
                    edges = self.adjacency[node_id]
                    for edge in edges:
                        target_id = edge['target']
                        handle = edge['handle']
                        
                        if node_type == 'logic':
                            # Route based on True/False
                            if node_result is True and handle == 'true':
                                next_layer.append(target_id)
                            elif node_result is False and handle == 'false':
                                next_layer.append(target_id)
                        elif node_type == 'loop':
                            # Route based on do/done
                            if node_result == 'do' and handle == 'do':
                                next_layer.append(target_id)
                            elif node_result == 'done' and handle == 'done':
                                next_layer.append(target_id)
                        else:
                            # Normal flow, take all connected edges
                            next_layer.append(target_id)
            
            # De-duplicate next layer to avoid processing same node twice in same step
            current_nodes = list(set(next_layer))
        
        return {
            "status": "success",
            "message": "Workflow completed without specific response",
            "logs": self.execution_log,
            "context": self.context
        }

    def _resolve_val(self, val):
        """Helper to substitute variables in a string or return the specific object from context if exact match."""
        if not isinstance(val, str):
            return val
        
        # Exact match (preserve type)
        if val.startswith('{') and val.endswith('}') and val.count('{') == 1:
            key = val[1:-1]
            return self.context.get(key, val)
        
        # Interpolation
        if '{' in val and '}' in val:
            for k, v in self.context.items():
                if f"{{{k}}}" in val:
                    val = val.replace(f"{{{k}}}", str(v))
            return val
        
        return val

    def execute_node(self, node: Dict[str, Any]):
        node_type = node['type']
        data = node.get('data', {})

        if node_type == 'api':
            pass
        
        elif node_type == 'variable':
            # Variable Node Execution (Init or Update)
            var_name = data.get('name')
            var_value = data.get('value')
            var_type = data.get('type', 'string')

            if var_name:
                # 1. Perform Variable Substitution if value is a string
                # This allows "Set Variable" to take values from previous nodes e.g. "{body.id}"
                if isinstance(var_value, str):
                    # Check for single variable replacement like "{myObj}" to preserve type
                    if var_value.startswith('{') and var_value.endswith('}') and var_value.count('{') == 1:
                        key = var_value[1:-1]
                        if key in self.context:
                            var_value = self.context[key]
                    else:
                        # Mixed string interpolation
                        for key, val in self.context.items():
                            placeholder = f"{{{key}}}"
                            if placeholder in var_value:
                                var_value = var_value.replace(placeholder, str(val))
                
                # 2. Type parsing for JSON/Array
                if var_type in ['json', 'array'] and isinstance(var_value, str):
                    try:
                        var_value = json.loads(var_value)
                    except:
                        # If parse fails, keep as string but maybe log warning?
                        # For now, simplistic approach.
                        pass

                self.context[var_name] = var_value
                self.execution_log.append(f"Set Variable '{var_name}' = {str(var_value)[:50]}...")

        elif node_type == 'logic':
            # Evaluate condition
            # Syntax: User might use JS "===" or "!==" or simple "x > 10"
            raw_condition = data.get('condition', 'False')
            
            # Simple sanitization/conversion for Python eval
            # Replace === with ==
            condition = raw_condition.replace('===', '==').replace('!==', '!=')
            
            try:
                # We pass 'self.context' as locals so variables are directly accessible by name
                # e.g. "myVar > 10" works if myVar is in context
                result = eval(condition, {"__builtins__": {}}, self.context)
                self.execution_log.append(f"Logic: '{raw_condition}' -> {bool(result)}")
                return {"type": "logic", "result": bool(result)}
            except Exception as e:
                self.execution_log.append(f"Logic Error: {e}")
                return {"type": "logic", "result": False}

        elif node_type == 'math':
            # Arithmetic Logic
            val_a = self._resolve_val(data.get('valA'))
            val_b = self._resolve_val(data.get('valB'))
            op = data.get('op', '+')
            result_var = data.get('resultVar', 'result')
            
            # Try to convert to numbers
            try:
                num_a = float(val_a)
                num_b = float(val_b)
                
                res = 0
                if op == '+': res = num_a + num_b
                elif op == '-': res = num_a - num_b
                elif op == '*': res = num_a * num_b
                elif op == '/': res = num_a / num_b if num_b != 0 else 0
                elif op == '%': res = num_a % num_b
                
                # If both were integers (e.g. 10.0), cast back to int for cleanliness?
                if num_a.is_integer() and num_b.is_integer():
                     res = int(res) if res != int(res) else res # wait, simple check:
                     if int(res) == res: res = int(res)

                self.context[result_var] = res
                self.execution_log.append(f"Math: {num_a} {op} {num_b} = {res}")
            except Exception:
                # Fallback to string operations for + or failure
                if op == '+':
                    res = str(val_a) + str(val_b)
                    self.context[result_var] = res
                    self.execution_log.append(f"Math (Str): {val_a} + {val_b} = {res}")
                else:
                    self.execution_log.append(f"Math Error: Could not process {val_a} {op} {val_b}")

        elif node_type == 'data_op':
            # Data Aggregation Logic
            collection_source = data.get('collection', '')
            op = data.get('op', 'sum')
            result_var = data.get('resultVar', 'summary')
            
            # 1. Resolve Collection (Same robustness as Loop)
            collection = self._resolve_val(collection_source)
            if isinstance(collection, str):
                if collection in self.context:
                    collection = self.context[collection]
                elif collection == 'body':
                     collection = self.context.get('body', [])
            
            if not isinstance(collection, list):
                 self.execution_log.append(f"DataNode Error: Input is not a list. Value: {str(collection)[:20]}")
                 collection = []

            # 2. Extract Numbers
            # Helper to get numeric values
            nums = []
            for x in collection:
                try:
                    nums.append(float(x))
                except:
                    pass
            
            res = 0
            if op == 'count':
                res = len(collection) # Count includes non-numbers
            elif op == 'sum':
                res = sum(nums)
            elif op == 'avg':
                res = sum(nums) / len(nums) if nums else 0
            elif op == 'min':
                res = min(nums) if nums else 0
            elif op == 'max':
                res = max(nums) if nums else 0
            
            # Clean int casting
            if isinstance(res, float) and res.is_integer():
                res = int(res)

            self.context[result_var] = res
            self.execution_log.append(f"Data Op: {op}(len={len(collection)}) = {res}")

        elif node_type == 'interface':
            # Schema Validation for Request Body
            fields = data.get('fields', [])
            body = self.context.get('body', {})
            
            missing = []
            invalid_types = []
            
            for field in fields:
                name = field.get('name')
                required = field.get('required', False)
                f_type = field.get('type', 'string')
                
                if not name:
                    continue
                
                if required and name not in body:
                    missing.append(name)
                    continue
                
                # Type Check (Optional but good)
                if name in body:
                    val = body[name]
                    if f_type == 'string' and not isinstance(val, str):
                        invalid_types.append(f"{name} (expected string)")
                    elif f_type == 'number' and not isinstance(val, (int, float)):
                         # Try parsing if it's a string number? No, strict usually.
                         # But let's be loose if it's a digit string? No, API usually strict.
                        invalid_types.append(f"{name} (expected number)")
                    elif f_type == 'boolean' and not isinstance(val, bool):
                        invalid_types.append(f"{name} (expected boolean)")
                    elif f_type == 'object' and not isinstance(val, dict):
                         invalid_types.append(f"{name} (expected object)")
                    elif f_type == 'array' and not isinstance(val, list):
                         invalid_types.append(f"{name} (expected array)")

            if missing or invalid_types:
                error_msg = "Validation Error: "
                if missing:
                    error_msg += f"Missing fields: {', '.join(missing)}. "
                if invalid_types:
                    error_msg += f"Invalid types: {', '.join(invalid_types)}."
                
                self.execution_log.append(f"Interface Validation Failed: {error_msg}")
                # Return immediate response which stops workflow
                return {
                    "type": "response", 
                    "data": {
                        "error": "Bad Request", 
                        "message": error_msg.strip(),
                        "details": {
                            "missing": missing,
                            "invalid": invalid_types
                        }
                    }
                }
            
            self.execution_log.append(f"Interface Validation Passed")

        elif node_type == 'loop':
            collection_source = data.get('collection', '')
            item_var = data.get('variable', 'item')
            
            # 1. First Pass: Resolve {variables} or keep string
            collection = self._resolve_val(collection_source)
            
            # 2. Second Pass: If string, try path lookup (body.items or direct key)
            if isinstance(collection, str):
                if collection.startswith('body.'):
                     path_parts = collection.split('.')
                     curr = self.context.get('body', {})
                     for part in path_parts[1:]:
                         if isinstance(curr, dict):
                             curr = curr.get(part)
                         else:
                             curr = []
                             break
                     collection = curr
                elif collection == 'body':
                     collection = self.context.get('body', [])
                elif collection in self.context:
                     collection = self.context[collection]
            
            # 3. Validation
            if not isinstance(collection, list):
                 self.execution_log.append(f"Loop Error: Collection '{collection_source}' resolved to {type(collection)}, expected list. Defaulting to empty.")
                 collection = []

            # Get State
            loop_states = self.context.setdefault('_loop_states', {})
            state = loop_states.get(node.get('id'), {'index': 0})
            
            idx = state['index']
            
            if idx < len(collection):
                # Do
                item = collection[idx]
                if item_var:
                    self.context[item_var] = item
                self.execution_log.append(f"Loop {node.get('id')}: Item {idx} = {str(item)[:20]}")
                
                # Increment
                state['index'] = idx + 1
                loop_states[node.get('id')] = state
                return {"type": "loop", "result": "do"}
            else:
                # Done
                self.execution_log.append(f"Loop {node.get('id')}: Done")
                # Reset for next run
                state['index'] = 0 
                loop_states[node.get('id')] = state
                return {"type": "loop", "result": "done"}

        elif node_type == 'function':
            func_name = data.get('name', 'func')
            self.execution_log.append(f"Ran function {func_name}")
            # Mock
        
        elif node_type == 'response':
            resp_type = data.get('responseType', 'json')
            body_def = data.get('body', '{}')
            
            if resp_type == 'variable':
                var_name = body_def
                val = self.context.get(var_name)
                if not val and isinstance(var_name, str) and var_name.startswith('{') and var_name.endswith('}'):
                     stripped = var_name.strip('{}')
                     val = self.context.get(stripped)
                return {"type": "response", "data": val}
            else:
                try:
                    # Recursive substitution function for nested dicts/lists
                    def substitute(obj):
                        if isinstance(obj, str):
                            # Check for straightforward single variable replacement like "{myObj}"
                            # to preserve the type (e.g. if myObj is a dict, return dict, not string representation)
                            if obj.startswith('{') and obj.endswith('}') and obj.count('{') == 1:
                                key = obj[1:-1]
                                if key in self.context:
                                    return self.context[key]
                            
                            # String interpolation for mixed content "Value is {val}"
                            for key, val in self.context.items():
                                placeholder = f"{{{key}}}"
                                if placeholder in obj:
                                     # Basic string replace
                                     if isinstance(val, (dict, list)):
                                         obj = obj.replace(placeholder, json.dumps(val))
                                     else:
                                         obj = obj.replace(placeholder, str(val))
                            return obj
                        elif isinstance(obj, dict):
                            return {k: substitute(v) for k, v in obj.items()}
                        elif isinstance(obj, list):
                            return [substitute(i) for i in obj]
                        return obj

                    # First try to parse the JSON skeleton
                    # Then substitute values inside
                    # OR substitute string first then parse? 
                    # Substituting string first is risky for quotes.
                    # BUt user inputs string.
                    
                    # Let's try naïve string sub first as before, but improved.
                    final_body = body_def
                    for key, val in self.context.items():
                        placeholder = f"{{{key}}}"
                        if placeholder in final_body:
                             if isinstance(val, (int, float, bool)):
                                 final_body = final_body.replace(placeholder, str(val).lower() if isinstance(val, bool) else str(val))
                             elif isinstance(val, (dict, list)):
                                 final_body = final_body.replace(placeholder, json.dumps(val))
                             else:
                                 # It's a string value. If we just insert it, we might break JSON if it has quotes.
                                 # e.g. "name": "{name}" -> "name": "tushar" (ok)
                                 # "name": "{name}" where name='a"b' -> "name": "a"b" (INVALID JSON)
                                 # Correct way is difficult with pure string replace on raw JSON.
                                 final_body = final_body.replace(placeholder, str(val))
                    
                    return {"type": "response", "data": json.loads(final_body)}
                except Exception as e:
                    self.execution_log.append(f"Error parsing response body: {e}")
                    return {"type": "response", "data": {"raw": body_def, "error": "JSON parse error"}}
        
        return None
