Tasks (specified in `tasks.py`) and components (specified in `components/`) are designed to be very loosely coupled from one another for the sake of modularity. One task can consist of multiple components via composition, so this loose coupling facilites a "plug-and-play" pattern where components can be swapped out for others to fit the configuration of the specific site the job is being run on.

This module houses stuff that is shared between tasks and components, such as type definitions, data schemas, and `helper` utilities.
