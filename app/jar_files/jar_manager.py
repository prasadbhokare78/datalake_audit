import os
# from app.constants.constant_error import JarManagerError

class JarManager:
    def __init__(self, required_jars):
        """
        Initialize JarManager to locate required JAR files.

        :param required_jars: List of required JAR filenames.
        """
        self.jar_folder = os.path.dirname(os.path.abspath(__file__))
        self.required_jars = required_jars

    def get_jars(self):
        """
        Retrieve full paths of required JAR files in the same folder as the script.

        :return: List of full paths to required JAR files.
        """
        jar_files = [file for file in os.listdir(self.jar_folder) if file.endswith('.jar')]
        missing_jars = [jar for jar in self.required_jars if jar not in jar_files]

        if missing_jars:
            raise Exception(str({', '.join(missing_jars)}))

        return [os.path.join(self.jar_folder, jar) for jar in self.required_jars]
