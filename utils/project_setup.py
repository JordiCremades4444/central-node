"""Class with ONLY setup methods
"""

import datetime
import os
import shutil


class Setup:
    def __init__(self, main_folder=None, sub_folder=None, params=None):
        """
        Description:
        ------------

        The init method of this function prepares the variables used to
        later create the reposotory of the task


        Parameters:
        ------------

        main_folder: String
            Name of the main folder
        sub_folder: String
            Name of the sub_folder
        params: Dictionary
            Dictionary with info of active and non active params
        """
        try:
            # info of current year
            current_year = datetime.datetime.now().year

            self.main_folder = main_folder
            self.sub_folder_raw = sub_folder
            self.sub_folder = f"{sub_folder}_{current_year}"
            self.main_folder_path = os.path.join(
                os.getcwd(), self.main_folder, self.sub_folder
            )
            self.params = params
        except Exception as e:
            print(f"An error occurred: {e}")

    def is_new(self):
        """
        Description:
        ------------

        This method checks if the main_folder and sub_folder specified
        in _init_ already exist.


        Parameters:
        ------------

        self:

        Returns:
        ------------

        Boolean
            True if the combination main_folder and sub_folder is new
        """
        try:
            # check if the main_folder/sub_folder combination already exist
            if os.path.exists(rf"{self.main_folder_path}"):
                print(self.main_folder_path)
                return False
            else:
                print(self.main_folder_path)
                return True  # the project is new
        except Exception as e:
            print(f"An error occurred: {e}")

    def create(self):
        """
        Description:
        ------------

        This method creates the folders, subfolders and other variables


        Parameters:
        ------------

        self:

        Returns:
        ------------

        Nothing, just builds the
        """
        try:
            is_new = self.is_new()

            # create the folders and extra only if its new
            if is_new:
                # create main folder
                os.makedirs(self.main_folder_path)

                # create a folder for each true value
                for key, value in self.params.items():
                    if value == True:  # check if the value is true
                        value_folder = os.path.join(self.main_folder_path, key)
                        os.makedirs(value_folder)

                # copy template
                template = f"knowledge\\templates\\{self.params['version']}.ipynb"
                source_template = os.path.join(os.getcwd(), template)
                destination_template = self.main_folder_path
                shutil.copy(rf"{source_template}", rf"{destination_template}")
            else:
                print("It already exists!")
        except Exception as e:
            print(f"An error occurred: {e}")
