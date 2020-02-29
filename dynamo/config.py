import json

class Config():
    def __init__(self, path):
        super().__init__()
        try:
            conf_file = open(path)
            json_f = json.load(conf_file)
            self.region = json_f['region']
        except:
            print('No such file')

    def get_region(self):
        return self.region
    
    def __str__(self):
        return "{}".format(self.region)