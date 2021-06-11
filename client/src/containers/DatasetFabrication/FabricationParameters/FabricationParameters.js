import React, { Component } from "react";
import classes from "./FabricationParameters.module.css"
import FabricationMethod from "./FabricationMethod/FabricationMethod";

class FabricationParameters extends Component {

    state = {
        joinable: {},
        unionable: {},
        viewUnionable: {},
        semanticallyJoinable: {}
    }

    getSelected(val, variant) {
        const updatedState = {...this.state}
        if (variant === "Joinable") {
            updatedState.joinable = {...val}
        } else if (variant === "Unionable") {
            updatedState.unionable = {...val}
        } else if (variant === "View Unionable") {
            updatedState.viewUnionable = {...val}
        } else if (variant === "Semantically Joinable") {
            updatedState.semanticallyJoinable = {...val}
        }
        this.props.sendSelected(updatedState)
        this.setState(updatedState)
    }


    render() {
        const fabricationVariant = ["Joinable", "Unionable", "View Unionable", "Semantically Joinable"]
        return(
            <div className={classes.FabricationMode}>
                {fabricationVariant.map((value) => {
                    return <FabricationMethod key={value}
                                              methodName={value}
                                              sendSelected={(val) => this.getSelected(val, value)}/>
                })}
            </div>
        );
    }

}

export default FabricationParameters;