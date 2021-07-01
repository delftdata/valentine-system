import React, {Component} from "react";

import classes from "./AlgorithmSelection.module.css";
import Algorithm from "./Algorithm/Algorithm";

class AlgorithmSelection extends Component {

    state = {
        comaParams:{
            defaultAlgoParams: {
                name:"Default Params",
                elementType: "checkbox",
                elementConfig:{
                    type: "checkbox",
                    defaultChecked: true,
                    name: "Default Parameters"
                },
                value: true,
                show: true
             },
            Coma_strategy: {
                name: "Strategy",
                elementType: "input",
                elementConfig: {
                    options: [
                        {value: "COMA_OPT", displayValue: "Schema"},
                        {value: "COMA_OPT_INST", displayValue: "Schema + Instances"}
                        ]
                },
                value: "",
                show: false
            },
            Coma_max_n: {
                name: "max_n",
                elementType: "input",
                elementConfig : {
                    min: 0,
                    max: 10,
                    step: 1,
                    defaultValue: 0
                },
                value: "",
                show: false
            }
        },
        cupidParams: {
             defaultAlgoParams: {
                name:"Default Params",
                elementType: "checkbox",
                elementConfig:{
                    type: "checkbox",
                    defaultChecked: true,
                    name: "Default Parameters"
                },
                value: true,
                show: true
             },
            Cupid_leaf_w_struct: {
                name: "leaf_w_struct",
                elementType: "input",
                elementConfig : {
                    min: 0.0,
                    max: 1.0,
                    step: 0.01,
                    defaultValue: 0.2
                },
                value: "",
                show: false
            },
            Cupid_w_struct: {
                name: "w_struct",
                elementType: "input",
                elementConfig : {
                    min: 0.0,
                    max: 1.0,
                    step: 0.01,
                    defaultValue: 0.2
                },
                value: "",
                show: false
            },
            Cupid_th_accept: {
                name: "th_accept",
                elementType: "input",
                elementConfig : {
                    min: 0.0,
                    max: 1.0,
                    step: 0.01,
                    defaultValue: 0.7
                },
                value: "",
                show: false
            },
            Cupid_th_high: {
                name: "th_high",
                elementType: "input",
                elementConfig : {
                    min: 0.0,
                    max: 1.0,
                    step: 0.01,
                    defaultValue: 0.6
                },
                value: '',
                show: false
            },
            Cupid_th_low: {
                name: "th_low",
                elementType: "input",
                elementConfig : {
                    min: 0.0,
                    max: 1.0,
                    step: 0.01,
                    defaultValue: 0.35
                },
                value: '',
                show: false
            },
            Cupid_th_ns: {
                name: "th_ns",
                elementType: "input",
                elementConfig : {
                    min: 0.0,
                    max: 1.0,
                    step: 0.01,
                    defaultValue: 0.7
                },
                value: "",
                show: false
            }
        },
        distributionBasedParams:{
            defaultAlgoParams: {
                name:"Default Params",
                elementType: "checkbox",
                elementConfig:{
                    type: "checkbox",
                    defaultChecked: true,
                    name: "Default Parameters"
                },
                value: true,
                show: true
             },
            CorrelationClustering_threshold1: {
                name: "threshold1",
                elementType: "input",
                elementConfig : {
                    min: 0.0,
                    max: 1.0,
                    step: 0.01,
                    defaultValue: 0.15
                },
                value: "",
                show: false
            },
            CorrelationClustering_threshold2: {
                name: "threshold2",
                elementType: "input",
                elementConfig : {
                    min: 0.0,
                    max: 1.0,
                    step: 0.01,
                    defaultValue: 0.15
                },
                value: "",
                show: false
            },
            CorrelationClustering_quantiles: {
                name: "quantiles",
                elementType: "input",
                elementConfig : {
                    min: 1,
                    max: 1024,
                    step: 1,
                    defaultValue: 256
                },
                value: "",
                show: false
            }
        },
        jaccardLevenParams: {
            defaultAlgoParams: {
                name:"Default Params",
                elementType: "checkbox",
                elementConfig:{
                    type: "checkbox",
                    defaultChecked: true,
                    name: "Default Parameters"
                },
                value: true,
                show: true
            },
            JaccardLevenMatcher_threshold_leven: {
                name: "threshold_leven",
                elementType: "input",
                value: 0.8,
                show: false
            }
        },
        embDIParams: {
            defaultAlgoParams: {
                name:"Default Params",
                elementType: "checkbox",
                elementConfig:{
                    type: "checkbox",
                    defaultChecked: true,
                    name: "Default Parameters"
                },
                value: true,
                show: true
            }
        },
        algorithmParams: {
            Coma: {max_n: 0, strategy: "COMA_OPT"},
            Cupid: {leaf_w_struct: 0.2, w_struct: 0.2, th_accept: 0.7, th_high: 0.6, th_low:0.35, th_ns: 0.7},
            SimilarityFlooding: {},
            CorrelationClustering: {threshold1: 0.15, threshold2: 0.15, quantiles: 256},
            JaccardLevenMatcher: {threshold_leven: 0.8},
            EmbDI: {}
        },
        isSelected:{
            Coma: false,
            Cupid: false,
            SimilarityFlooding: false,
            CorrelationClustering: false,
            JaccardLevenMatcher: false,
            EmbDI: false
        }
    }

    getSelectedAlgorithms(val, algorithmName){
        const algoParams = {}
        if(val.hasOwnProperty("defaultAlgoParams") && !val["defaultAlgoParams"].value) {
            for (let key in val) {
                if (val.hasOwnProperty(key)) {
                    if (key.startsWith(algorithmName)) {
                        algoParams[key.substr(algorithmName.length + 1)] = val[key].value;
                    }
                }
            }
            const localAlgorithmParams = {...this.state.algorithmParams};
            localAlgorithmParams[algorithmName] = algoParams;
            this.setState({algorithmParams: localAlgorithmParams}, () => this.sendSelectedToParent());
        }else{
             this.sendSelectedToParent();
        }
    }

    sendSelectedToParent = () => {
        const selectedAlgorithms = [];
         for (let algorithmName in this.state.isSelected) {
             if(this.state.isSelected[algorithmName]){
                 let selectedAlgorithm = {[algorithmName]: this.state.algorithmParams[algorithmName]};
                 selectedAlgorithms.push(selectedAlgorithm);
             }
         }
        this.props.sendSelected(selectedAlgorithms);
    }

    toggleAlgorithmSelection = (selected, algorithmName) => {
        const localIsSelected = {...this.state.isSelected};
        localIsSelected[algorithmName] = selected;
        this.setState({isSelected: localIsSelected}, () => this.sendSelectedToParent());
    }

    render() {
        return(
            <div className={classes.Algorithms}>
                <h5>Select algorithms to run</h5>
                <p className={classes.Description}>For the grid search add parameter values separated by coma.
                    For range definitions use the notation min:step:max
                </p>
                <div className={classes.Algorithm}>
                    <Algorithm algoName={"Coma"} params={this.state.comaParams}
                               sendSelected={(val) => this.getSelectedAlgorithms(val, "Coma")}
                               toggleAlgorithm={(selected) => this.toggleAlgorithmSelection(selected, "Coma")}
                    />
                </div>
                <div className={classes.Algorithm}>
                    <Algorithm algoName={"Cupid"} params={this.state.cupidParams}
                               sendSelected={(val) => this.getSelectedAlgorithms(val, "Cupid")}
                               toggleAlgorithm={(selected) => this.toggleAlgorithmSelection(selected, "Cupid")}
                    />
                </div>
                <div className={classes.Algorithm}>
                    <Algorithm algoName={"Distribution Based"} params={this.state.distributionBasedParams}
                               sendSelected={(val) => this.getSelectedAlgorithms(val, "CorrelationClustering")}
                               toggleAlgorithm={(selected) => this.toggleAlgorithmSelection(selected, "CorrelationClustering")}
                    />
                </div>
                <div className={classes.Algorithm}>
                    <Algorithm algoName={"Jaccard Levenshtein"} params={this.state.jaccardLevenParams}
                               sendSelected={(val) => this.getSelectedAlgorithms(val, "JaccardLevenMatcher")}
                               toggleAlgorithm={(selected) => this.toggleAlgorithmSelection(selected, "JaccardLevenMatcher")}
                    />
                </div>
                <div className={classes.Algorithm}>
                     <Algorithm algoName={"Similarity Flooding"} params={null}
                                sendSelected={(val) => this.getSelectedAlgorithms(val, "SimilarityFlooding")}
                                toggleAlgorithm={(selected) => this.toggleAlgorithmSelection(selected, "SimilarityFlooding")}
                     />
                </div>
                <div className={classes.Algorithm}>
                     <Algorithm algoName={"EmbDI"} params={this.state.embDIParams}
                                sendSelected={(val) => this.getSelectedAlgorithms(val, "EmbDI")}
                                toggleAlgorithm={(selected) => this.toggleAlgorithmSelection(selected, "EmbDI")}
                     />
                </div>
            </div>
        );
    }
}

export default AlgorithmSelection;