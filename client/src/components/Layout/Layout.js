import React from "react";

import Aux from "../../hoc/Hoc";
import classes from "./Layout.module.css";
import Toolbar from "../Navigation/Toolbar/Toolbar";

const layout = (props) => (
    <Aux>
        <Toolbar toolbar_elements={props.toolbar_elements}/>
        <main className={classes.Content}>
            {props.children}
        </main>
    </Aux>
);

export default layout;