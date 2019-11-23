""" Airtunnel operators for loading (i.e. getting from staging/ready to ready in the data store) tasks. """
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import airtunnel.data_store
import airtunnel.metadata.adapter
import airtunnel.operators
from airtunnel.data_asset import BaseDataAsset
from airtunnel.metadata.entities import LoadStatus


@apply_defaults
class StagingToReadyOperator(BaseOperator):
    """ Airtunnel's StagingToReadyOperator – moves staged files (from staging/read) to ready for a data asset and
       write load status metadata.
    """

    ui_color = airtunnel.operators.Colours.loading

    @apply_defaults
    def __init__(self, asset: BaseDataAsset, *args, **kwargs):
        self._asset = asset
        self._data_store_adapter = airtunnel.data_store.get_configured_adapter()

        if "task_id" not in kwargs:
            kwargs["task_id"] = asset.name + "_" + "staging_to_ready"

        super().__init__(*args, **kwargs)

    def execute(self, context):
        """ Execute this operator from Airflow. """
        # python does not have a straight forward "move and overwrite":
        # https://stackoverflow.com/questions/7419665/python-move-and-overwrite-files-and-folders
        # hence, we use a workaround, which is unfortunately less atomic. keep in mind for cloud storage!

        moved_to_temp_path = False
        move_to_ready_succeeded = False
        asset_temp_path = None

        try:
            self._data_store_adapter.makedirs(
                path=self._asset.ready_path, exist_ok=True
            )

            self.log.info(f"Loading new version to {self._asset.ready_path}")
            asset_temp_path = self._asset.make_ready_temp_path(context)

            self._data_store_adapter.move(
                source=self._asset.ready_path,
                destination=asset_temp_path,
                recursive=True,
            )

            moved_to_temp_path = True

            # load the prepared data
            self._data_store_adapter.move(
                source=self._asset.staging_ready_path,
                destination=self._asset.ready_path,
                recursive=True,
            )

            move_to_ready_succeeded = True

        except Exception as e:
            raise e

        finally:
            # depending on a successful move or not, we have to leave a consistent state in ready:
            if moved_to_temp_path and move_to_ready_succeeded:
                try:
                    # log load-status
                    airtunnel.metadata.adapter.get_configured_adapter().write_load_status(
                        LoadStatus(
                            for_asset=self._asset,
                            dag_id=self.dag_id,
                            task_id=self.task_id,
                            dag_exec_date=context["task_instance"].execution_date,
                        )
                    )
                    self.log.info(
                        f"Successfully loaded - removing old copy at temp location: {asset_temp_path}"
                    )
                except Exception as e:
                    self.log.warning(f"Error on logging load status: {e}")
                # we can safely remove the old version
                self._data_store_adapter.delete(path=asset_temp_path, recursive=True)

            elif moved_to_temp_path and not move_to_ready_succeeded:
                self.log.warning(
                    f"Error on loading - restoring old version from {asset_temp_path}"
                )
                # we have to revert to the old version
                self._data_store_adapter.move(
                    source=asset_temp_path,
                    destination=self._asset.ready_path,
                    recursive=True,
                )
