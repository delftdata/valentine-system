import React, {Component} from "react";
import classes from "./FabricationMethod.module.css";
import Checkbox from "@material-ui/core/Checkbox";
import {TextField} from "@material-ui/core";


class FabricationMethod extends Component{

    state = {
        selected: false,
        vInstances: true,
        nInstances: true,
        vSchemata: true,
        nSchemata: true,
        numberOfPairs: 0,
        params: {}
    }

    componentDidMount() {
        this.setState({params: {...this.props.params}})
    }


    toggleSelected = () => {
        this.setState({selected: !this.state.selected})
    }

    toggleNoisyInstances = () => {
        this.setState({nInstances: !this.state.nInstances})

    }

    toggleVerbatimInstances = () => {
        this.setState({vInstances: !this.state.vInstances})

    }

    toggleNoisySchemata = () => {
        this.setState({nSchemata: !this.state.nSchemata})
    }

    toggleVerbatimSchemata = () => {
        this.setState({vSchemata: !this.state.vSchemata})
    }

    render() {

        const noisyInstances = <div className={classes.Choice}>
            {(this.props.methodName !== "Joinable" && this.props.methodName !== "Semantically Joinable") ?
                <Checkbox
                    checked={this.state.nInstances}
                    onChange={() => this.toggleNoisyInstances()}
                    color="primary"
                /> :
                (this.props.methodName === "Joinable") ?
                    <Checkbox disabled inputProps={{ 'aria-label': 'disabled checked checkbox' }} />
                    : <Checkbox disabled checked inputProps={{ 'aria-label': 'disabled checked checkbox' }} />
                }
                <p>Noisy instances</p>
            </div>;

        const verbatimInstances = <div className={classes.Choice}>
            {(this.props.methodName !== "Joinable" && this.props.methodName !== "Semantically Joinable") ?
                <Checkbox
                    checked={this.state.vInstances}
                    onChange={() => this.toggleVerbatimInstances()}
                    color="primary"
                /> :
                (this.props.methodName === "Joinable") ?
                    <Checkbox disabled checked inputProps={{ 'aria-label': 'disabled checked checkbox' }} />
                    : <Checkbox disabled inputProps={{ 'aria-label': 'disabled checked checkbox' }} />
                }
                <p>Verbatim instances</p>
            </div>;

        return(
            <div className={classes.FabricationMethod}>
                <div className={classes.Header}>
                    <Checkbox
                        checked={this.state.selected}
                        onChange={() => this.toggleSelected()}
                        color="primary"
                    />
                    <h4>{this.props.methodName}</h4>
                </div>
                <div>
                    <div className={classes.TextField}>
                        <TextField id="standard-basic" label="Number of pairs" />
                    </div>
                    <div className={classes.IncludeHeader}>
                        <h5> Include: </h5>
                    </div>
                    {noisyInstances}
                    <div className={classes.Choice}>
                        <Checkbox
                            checked={this.state.nSchemata}
                            onChange={() => this.toggleNoisySchemata()}
                            color="primary"
                        />
                        <p>Noisy schemata</p>
                    </div>
                    {verbatimInstances}
                    <div className={classes.Choice}>
                        <Checkbox
                            checked={this.state.vSchemata}
                            onChange={() => this.toggleVerbatimSchemata()}
                            color="primary"
                        />
                        <p>Verbatim schemata</p>
                    </div>
                </div>
            </div>

        );
    }


}

export default FabricationMethod;