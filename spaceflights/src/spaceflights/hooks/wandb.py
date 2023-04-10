import os
import logging

from kedro.framework.hooks import hook_impl
from kedro.io.data_catalog import DataCatalog
from kedro.config import ConfigLoader
from kedro.framework.project import settings

import wandb


class WandbHooks:
    @property
    def _logger(self):
        return logging.getLogger(__name__)

    @hook_impl
    def before_pipeline_run(self, run_params: dict):
        project_path = run_params["project_path"]
        env = run_params["env"] or "local"
        
        conf_path = os.path.join(project_path, settings.CONF_SOURCE)
        conf_loader = ConfigLoader(conf_source=conf_path, env=env, **settings.CONFIG_LOADER_ARGS)

        wandb_params = conf_loader["wandb"]
        self._wandb_active: bool = wandb_params["active"]
        self._wandb_project: str = wandb_params["project"]

    @hook_impl
    def before_node_run(self, catalog: DataCatalog, inputs: dict) -> None:
        if self._wandb_active:
            wandb.init(project=self._wandb_project)
    
    @hook_impl
    def after_node_run(self) -> None:
        if self._wandb_active:
            wandb.finish()
