# dags/wayfair/common_etl.py
import os
import json
from datetime import datetime
from utils.common.metadata_manager import get_latest_folder
import logging
from typing import List, Dict, Any, Union

logging.basicConfig(level=logging.DEBUG)

def get_product_variations(
    path: str = None, 
    has_variations: bool = True
) -> Union[List[Dict[str, Any]], List[str]]:
    """
    Extracts product variations from raw product detail JSON files.

    Args:
        path (str, optional): Folder path to JSON files. If not provided, the latest folder is used.
        has_variations (bool): 
            - If True: return full variation info (sku, options, url).
            - If False: return only the list of unique SKUs.

    Returns:
        Union[List[Dict], List[str]]: 
            - A list of variation dicts (with SKU, options, and URL), or
            - A list of unique SKU strings (if has_variations is False).

    Details:
        - Each product JSON file may contain an 'optionComboToPartId' field (variations).
        - Variations are parsed based on that field. If it's a dict, each key represents a variation combination.
        - Deduplication is handled via sets.
        - Invalid or unreadable files are logged and skipped.
    """

    if path is None:
        path = get_latest_folder(base_dir='data/wayfair') + '/product_detail/product_detail_page/'
    
    variations = []
    skus = set()  # Use set for automatic deduplication
    try:
        files = [f for f in os.listdir(path)   if f.endswith('.json') or os.path.isdir(os.path.join(path, f)) ]
        logging.info(f"Processing {len(files)} JSON files from {path}")
        
        for file in files:
            file_path = os.path.join(path, file)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                sku = data.get('sku')
                url = data.get('url')
                if not sku:
                    logging.warning(f"No SKU found in file: {file}")
                    continue
                    
                skus.add(sku)
                if has_variations:
                    raw_variations = data.get('optionComboToPartId')
                    if isinstance(raw_variations, list):
                        variations.append({'sku': sku, 'options': [], 'url': url})
                    elif raw_variations and isinstance(raw_variations, dict):
                        for variation in raw_variations.keys():
                            variation_options = [var for var in variation.split('-') if var]
                            variations.append({'sku': sku, 'options': variation_options, 'url': url})
                            
            except (json.JSONDecodeError, IOError) as e:
                logging.error(f"Error processing file {file}: {e}")
                continue
    
    except OSError as e:
        logging.error(f"Error accessing directory {path}: {e}")
        return [] if has_variations else []
    
    if not has_variations:
        skus_list = list(skus)
        logging.info(f'Number of unique SKUs: {len(skus_list)}')
        return skus_list
    
    logging.info(f'Number of variations: {len(variations)}')
    return variations

def get_success_product_variations(
    path: str = None, 
    has_variations: bool = True
) -> Union[List[Dict[str, Any]], List[str]]:
    """
    Parses successfully crawled product variations from file names (e.g., after crawling dimensions/specs).

    Args:
        path (str, optional): Path to the processed output files. Defaults to latest product_dimensions folder.
        has_variations (bool):
            - If True: return parsed variations from filenames.
            - If False: return only the list of unique SKUs.

    Returns:
        Union[List[Dict], List[str]]: 
            - List of variation dicts (parsed from filename), or
            - List of unique SKU strings (if has_variations is False).

    Details:
        - File names are expected in the format: sku_option1_option2.json or .html.
        - Skips empty or malformed filenames gracefully.
    """

    if path is None:
        path = get_latest_folder(base_dir='data/wayfair') + '/product_detail/product_dimensions/'
    
    variations = []
    skus = set()
    
    try:
        files = [f for f in os.listdir(path) if f.endswith('.json') or os.path.isdir(os.path.join(path, f)) or f.endswith('.html')]
        logging.info(f"Processing {len(files)} success files from {path}")
        
        for file in files:
            try:
                # Parse filename: sku_option1_option2_...json
                file_base = file.replace('.json', '').replace('.html', '')
                parts = file_base.split('_')    
                
                if not parts:
                    continue
                    
                sku = parts[0]
                skus.add(sku)
                
                if has_variations:
                    raw_variations = parts[1:] if len(parts) > 1 else []
                    variations.append({'sku': sku, 'options': raw_variations})
                    
            except Exception as e:
                logging.warning(f"Error parsing success file {file}: {e}")
                continue
                
    except OSError as e:
        logging.error(f"Error accessing success directory {path}: {e}")
        return [] if has_variations else []
    
    if not has_variations:
        skus_list = list(skus)
        logging.info(f'Number of successful unique SKUs: {len(skus_list)}')
        return skus_list
    
    logging.info(f'Number of successful variations: {len(variations)}')
    return variations

def _get_variation_key(variation: Union[Dict[str, Any], str]) -> str:
    """
    Generates a unique string key for a variation or SKU.

    Args:
        variation (Union[Dict, str]):
            - If dict: expects {'sku': ..., 'options': [...]}
            - If str: treat as SKU-only (no variations)

    Returns:
        str: Unique key in the format 'sku_option1_option2', or just 'sku' if no options.

    Used to:
        - Normalize variation format for comparison (e.g., detect duplicates or match success/failure).
    """

    if isinstance(variation, str):
        return variation
        
    sku = variation['sku']
    options = variation.get('options', [])
    
    if options:
        selected_option_ids = '_'.join(str(opt) for opt in options if opt)
        return f"{sku}_{selected_option_ids}"
    return sku

def get_failed_product_variations(
    all_variations: Union[List[Dict[str, Any]], List[str]], 
    success_variations: Union[List[Dict[str, Any]], List[str]], 
    has_variations: bool = True
) -> Union[List[Dict[str, Any]], List[str]]:
    """
    Identifies failed product variations by comparing all collected variations to successfully processed ones.

    Args:
        all_variations (List): All variations or SKUs originally collected.
        success_variations (List): Variations or SKUs that were successfully processed.
        has_variations (bool): 
            - If True: handle full variations with options.
            - If False: compare just SKUs.

    Returns:
        Union[List[Dict], List[str]]:
            - A list of failed variations (dicts with sku, options, and url if available), or
            - A list of failed SKUs (str) if has_variations is False.

    Details:
        - Uses `_get_variation_key()` to generate comparable keys for each variation.
        - Logs the number of failed items for easy tracking/debugging.
    """

    if not has_variations:
        # Simple set subtraction for SKUs
        all_keys = set(all_variations) if isinstance(all_variations, list) else set()
        success_keys = set(success_variations) if isinstance(success_variations, list) else set()
        failed_keys = all_keys - success_keys

        failed_list = list(failed_keys)
        logging.info(f"❌ Number of failed SKUs: {len(failed_list)}")
        return failed_list

    # Handle variations
    all_key_map = {_get_variation_key(v): v for v in all_variations}
    all_keys = set(all_key_map.keys())
    success_keys = set(_get_variation_key(v) for v in success_variations)
    failed_keys = all_keys - success_keys

    failed_variations = []
    for key in failed_keys:
        try:
            v = all_key_map[key]
            failed_variations.append({
                'sku': v['sku'],
                'options': v.get('options', []),
                'url': v.get('url')
            })
        except Exception as e:
            logging.warning(f"Error retrieving failed variation for key {key}: {e}")
            continue

    logging.info(f"❌ Number of failed variations: {len(failed_variations)}")
    return failed_variations





# if __name__ == "__main__":
#     all_variations = get_product_variations(path='data/wayfair/2025/6/19/product_detail/product_detail_page')
#     success_variations = get_success_product_variations(path='data/wayfair/2025/6/19/product_detail/product_src_page')
#     failed_variations = get_failed_product_variations(all_variations, success_variations)

#     import json
#     with open('data/wayfair/2025/6/19/product_detail/failed_variations.json', 'w') as f:
#         json.dump(failed_variations, f, indent=4)