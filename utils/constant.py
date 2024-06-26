from configparser import ConfigParser
import json
config_parser=ConfigParser()

config_parser.read('../config/config.ini')

api_key = config_parser.get('API', "api_key")

print(api_key)
ticker=config_parser.get("finhub", "finhub_ticker")


avro_schema=json.dumps({
    "namespace":"kafka.broker.producer",
    "type":"record",
    "name":"Stock",
    "fields":[
        {
            "name":"data",
            "type":{
                "type":"array",
                "items":{
                    "type":"record",
                    "name":"data",
                    "fields":[
                        {
                            "name": "c",
                            "type": [
                                {
                                    "type":"array",
                                    "items":["null","string"],
                                    "default":[]
                                },
                                "null"
                            ],
                            "doc":"les conditions de trading du symbole"
                        },
                        {
                            "name": "s",
                            "type": "string",
                            "doc":"symbol qui est tradé"
                        },
                        {
                            "name": "p",
                            "type": "float",
                            "description": "le prix du symbole"
                        },
                        {
                            "name": "t",
                            "type": "long",
                            "description": "le timestampe pour le trading du sysmbole"
                        },
                        {
                            "name": "v",
                            "type": "float",
                            "description": "le volume d'échange du symbole"
                        }
                    ]
                }
            }
        },
        {
            "name": "type",
            "type": "string",
            "doc": "Type of message"
        }     
    ],
    "doc":"les données de trading des symboles et le type"
}

)
