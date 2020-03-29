from configparser import ConfigParser
from typing import List

class GDConfig(ConfigParser):
    def __init__(self, *args, **kwargs):
        kwargs['allow_no_value'] = True
        super().__init__(*args, **kwargs)

    def getlist(self, section: str, option: str, *args, **kwargs) -> List[str]:
        """
        This will return the value as a list of strings from a comma-separated
        list of values
        """
        val = self.get(section, option)

        return [v.strip() for v in val.split(',')]
