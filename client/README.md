# User Interface 

This module contains the React.js app displays the UI of our system.

## UI Components

The UI components are split in two categories Components (stateless) Containers (stateful) and are listed below.

### Components

*   `Forms` Basic form input and output covering all input elements.
*   `Layout` Details the layout of the UI.
*   `Navigation` Contains the navigation logic between the different "pages" and the toolbar.
*   `UI` Various UI misc elements like the backdrop, modal, progressbar and spinner.

### Containers 

*   `Matcher` Contains the logic for the matcher "page" i.e., algorithm selection and listing of the data sources 
     with data selection.
    
*   `Results` Contains the logic for the schema matching job results "page" with the individual job's match results and 
     column data preview.
    
*   `VerifiedMatches` Contains the logic for the verified matches "page".

## Module's structure

*   `nginx` Folder containing the nginx server configuration.
    
*   `public` Folder containing the public assets of the front end.
    
*   `src` Folder containing the application code.
    
    *   `components` Folder containing the stateless UI components.
    *   `containers` Folder containing the stateful UI components.
    *   `hoc` Module containing the helper higher-order components of the UI.