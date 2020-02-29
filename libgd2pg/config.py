from configparser import ConfigParser

class GDConfig(ConfigParser):
    def getlist(self, section, option):
        """
        This will return the value as a list of strings from a comma-separated
        list of values
        """
        val = self.get(section, option)

        return [v.strip() for v in val.split(',')]
