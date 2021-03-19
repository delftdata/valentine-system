import React, {Component} from "react";

import classes from "./Algorithm.module.css";
import Checkbox from "@material-ui/core/Checkbox";
import Input from "../../../../components/Forms/Input/Input";


class Algorithm extends Component{

    state = {
        selected: false,
        params: {}
    }

    componentDidMount() {
        this.setState({params: {...this.props.params}})
    }

    toggle_show_dynamic_component = (updatedJobForm) => {
        for (let key in updatedJobForm) {
            if (updatedJobForm.hasOwnProperty(key)){
                if (!key.startsWith("defaultAlgoParams")) {
                    updatedJobForm[key].show = !updatedJobForm[key].show;
                }
            }
        }
    }

    sendSelectedToParent = () => {
        const paramsToPropagate = (this.state.params.hasOwnProperty("defaultAlgoParams") &&
                                   this.state.params["defaultAlgoParams"].value) ? {} : {...this.state.params};
        if(this.state.selected){
            this.props.sendSelected(paramsToPropagate);
        }
        this.props.toggleAlgorithm(this.state.selected);
    }

    inputChangedHandler = (event, inputIdentifier) => {
        const updatedJobForm = {
            ...this.state.params
        };
        const updatedJobElement = {
            ...updatedJobForm[inputIdentifier]
        };
        if(inputIdentifier === "defaultAlgoParams") {
            updatedJobElement.value = !updatedJobElement.value;
            this.toggle_show_dynamic_component(updatedJobForm);
        }else{
            if (event.target.textContent){
                updatedJobElement.value = parseFloat(event.target.textContent);
            } else {
                 updatedJobElement.value = event.target.value;
            }
        }
        updatedJobForm[inputIdentifier] = updatedJobElement;
        this.setState({params: updatedJobForm}, () => this.sendSelectedToParent());
    }

    toggleSelected = () => {
        this.setState({selected: !this.state.selected}, () => this.sendSelectedToParent());
    }

    render() {
        const formElementsArray = [];
        for (let key in this.state.params){
            if (this.state.params[key].show){
                formElementsArray.push({
                    id: key,
                    config: this.state.params[key]
                });
            }
        }
        return(
            <div>
                <div className={classes.Header}>
                    <Checkbox
                        checked={this.state.selected}
                        onChange={() => this.toggleSelected()}
                        color="primary"
                    />
                    <h4>{this.props.algoName}</h4>
                </div>
                {formElementsArray.map(formElement => (
                            <Input
                                key={formElement.id}
                                elementType={formElement.config.elementType}
                                config={formElement.config.elementConfig}
                                name={formElement.config.name}
                                value={formElement.config.value}
                                changed={(event) => this.inputChangedHandler(event, formElement.id)}/>
                                ))
                }
            </div>
        );
    }
}

export default Algorithm;