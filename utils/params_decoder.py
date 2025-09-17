from urllib.parse import urlparse, parse_qs


class ParamsDecoder:
    def __init__(self, url: str):
        self.url = url

    def convert_params_to_dict(self):
        params = dict(parse_qs(urlparse(self.url).query))
        return {k: v[0] for k, v in params.items()}
# convinient functions

def convert_params_to_dict(url: str):
    return ParamsDecoder(url).convert_params_to_dict()






    

