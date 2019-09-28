import os
import shutil

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import airtunnel.operators
from airtunnel.data_asset import BaseDataAsset
from airtunnel.metadata.adapter import SQLMetaAdapter
from airtunnel.metadata.entities import LoadStatus


@apply_defaults
class StagingToReadyOperator(BaseOperator):
    """ moves staged files to ready for a data asset, write load status metadata.
       if exists, moves prepared archive to final location. """
    ui_color = airtunnel.operators.Colours.loading
    @apply_defaults
    def __init__(self, asset: BaseDataAsset, *args, **kwargs):
        self._asset = asset

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "staging_to_ready"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        # python does not have a straight forward "move and overwrite":
        # https://stackoverflow.com/questions/7419665/python-move-and-overwrite-files-and-folders
        # hence, we use a workaround, which is unfortunately less atomic. keep in mind for cloud storage!

        # if there, rename the existing data asset folder

        moved_to_temp_path = False
        move_to_ready_succeeded = False
        asset_temp_path = None

        try:

            if os.path.exists(self._asset.ready_path):
                self.log.info(f"Loading new version to {self._asset.ready_path}")
                asset_temp_path = self._asset.make_ready_temp_path(context)
                shutil.move(self._asset.ready_path, asset_temp_path)
                moved_to_temp_path = True

            # load the prepared data
            os.rename(self._asset.staging_ready_path, self._asset.ready_path)
            move_to_ready_succeeded = True

        except Exception as e:
            raise e

        finally:
            # depending on a successful move or not, we have to leave a consistent state in ready:
            if moved_to_temp_path and move_to_ready_succeeded:
                try:
                    # log load-status
                    # todo: have a config and allow to dynamically load meta adapter
                    SQLMetaAdapter().write_load_status(
                        LoadStatus(for_asset=self._asset)
                    )
                    self.log.info(
                        f"Successfully loaded - removing old copy at temp location: {asset_temp_path}"
                    )
                except Exception as e:
                    self.log.warning(f"Error on logging load status: {e}")
                # we can safely remove the old version
                shutil.rmtree(path=asset_temp_path)
            elif moved_to_temp_path and not move_to_ready_succeeded:
                self.log.warning(
                    f"Error on loading - restoring old version from {asset_temp_path}"
                )
                # we have to revert to the old version
                shutil.move(asset_temp_path, self._asset.ready_path)
