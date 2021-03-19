import React, { Component } from "react";
import classes from "./FabricationParameters.module.css"
import FabricationMethod from "./FabricationMethod/FabricationMethod";

class FabricationParameters extends Component {

    state = {
        isSelected:{
            "Joinable": false,
            "Unionable": false,
            "View Unionable": false,
            "Semantically Joinable": false
        }
    }

    render() {
        return(
            <div className={classes.FabricationMode}>
                {Object.keys(this.state.isSelected).map((value) => {
                    return <FabricationMethod methodName={value}/>
                })}
            </div>
        );
    }

}

export default FabricationParameters;