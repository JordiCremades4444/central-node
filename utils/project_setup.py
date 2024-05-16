"""Class with ONLY setup methods
"""

import datetime
import os
import shutil


class Setup:
    def __init__(self, sub_folder=None, params=None):
        """
        Description:
        ------------

        The init method of this function prepares the variables used to
        later create the reposotory of the task


        Parameters:
        ------------

        sub_folder: String
            Name of the sub_folder
        params: Dictionary
            Dictionary with info of active and non active params
        """
        try:
            # info of current year
            current_year = datetime.datetime.now().year
            current_month = datetime.datetime.now().month

            self.sub_folder_raw = sub_folder
            self.sub_folder = f"{current_year}_{current_month}_{sub_folder}"
            self.main_folder_path = os.path.join(os.getcwd(), "tmp", self.sub_folder)
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
                return False
            else:
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
                template = f"utils\\templates\\template.ipynb"
                source_template = os.path.join(os.getcwd(), template)
                destination_template = self.main_folder_path
                shutil.copy(rf"{source_template}", rf"{destination_template}")

                # rename name
                just_copied_file = os.path.join(
                    os.getcwd(), self.main_folder_path, "template.ipynb"
                )
                new_name_copied_file = os.path.join(
                    os.getcwd(), self.main_folder_path, self.sub_folder_raw + ".ipynb"
                )
                os.rename(just_copied_file, new_name_copied_file)
            else:
                print("It already exists! Nothing was done.")
        except Exception as e:
            print(f"An error occurred: {e}")
