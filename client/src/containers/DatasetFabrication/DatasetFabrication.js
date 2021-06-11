import React, {Component} from "react";
import classes from "./DatasetFabrication.module.css";
import {Button, TextField} from "@material-ui/core";
import Aux from "../../hoc/Aux";
import FabricationParameters from "./FabricationParameters/FabricationParameters";
import Modal from "../../components/UI/Modal/Modal";
import Spinner from "../../components/UI/Spinner/Spinner";
import Response from "../../components/Forms/Response/Response";
import axios from "axios";


class DatasetFabrication extends Component {

    state = {
        loading: false,
        responseReceived: false,
        fileToBeSent: null,
        datasetGroupName: '',
        selectedVariants: {},
        latestResponse: ''
    }

    getSelected(val, mode){
        if(mode==="fabricationParams"){
            this.setState({selectedVariants: {...val}});
        }else if(mode==="file"){
            this.setState({fileToBeSent: val});
        }
    }

    changeFileHandler = (event) => {
        event.preventDefault();
        this.setState({fileToBeSent: event.target.files[0]})
	};

    changeDGNameHandler = (event) => {
        event.preventDefault();
        this.setState({datasetGroupName: event.target.value})
    }

    checkIfVariantSelected(){
        for (const value of Object.values(this.state.selectedVariants)) {
            if (value.selected){
                return true;
            }
        }
        return false;
    }

    sendJob = () => {
        if (!this.checkIfVariantSelected()) {
            alert("You have not selected any fabrication variant!");
            return;
        }
        if (this.state.fileToBeSent == null) {
            alert("You have not selected a file!");
            return;
        }
        if (this.state.datasetGroupName===''){
            alert("You have not given a dataset group name!");
            return;
        }
        const formData = new FormData();
        formData.append("resource", this.state.fileToBeSent);
        formData.append("dataset_group_name", this.state.datasetGroupName);
        if (this.state.selectedVariants.joinable && this.state.selectedVariants.joinable.selected) {
            if (this.state.selectedVariants.joinable.numberOfPairs === 0) {
                alert("You have selected the Joinable variant without specifying the number of pairs!");
                return;
            }
            formData.append("fabricate_joinable", this.state.selectedVariants.joinable.selected)
            const nInstances = this.state.selectedVariants.joinable.nInstances
            const nSchemata = this.state.selectedVariants.joinable.nSchemata
            const vInstances = this.state.selectedVariants.joinable.vInstances
            const vSchemata = this.state.selectedVariants.joinable.vSchemata
            formData.append("joinable_specs-0", nInstances)
            formData.append("joinable_specs-1", nSchemata)
            formData.append("joinable_specs-2", vInstances)
            formData.append("joinable_specs-3", vSchemata)
            formData.append("joinable_pairs", this.state.selectedVariants.joinable.numberOfPairs)
        }
        if (this.state.selectedVariants.unionable && this.state.selectedVariants.unionable.selected) {
            if (this.state.selectedVariants.unionable.numberOfPairs === 0) {
                alert("You have selected the Unionable variant without specifying the number of pairs!");
                return;
            }
            formData.append("fabricate_unionable", this.state.selectedVariants.unionable.selected)
            const nInstances = this.state.selectedVariants.unionable.nInstances
            const nSchemata = this.state.selectedVariants.unionable.nSchemata
            const vInstances = this.state.selectedVariants.unionable.vInstances
            const vSchemata = this.state.selectedVariants.unionable.vSchemata
            formData.append("unionable_specs-0", nInstances)
            formData.append("unionable_specs-1", nSchemata)
            formData.append("unionable_specs-2", vInstances)
            formData.append("sunionable_specs-3", vSchemata)
            formData.append("unionable_pairs", this.state.selectedVariants.unionable.numberOfPairs)
        }
        if (this.state.selectedVariants.semanticallyJoinable && this.state.selectedVariants.semanticallyJoinable.selected) {
            if (this.state.selectedVariants.semanticallyJoinable.numberOfPairs === 0) {
                alert("You have selected the Semantically Joinable variant without specifying the number of pairs!");
                return;
            }
            formData.append("fabricate_semantically_joinable", this.state.selectedVariants.semanticallyJoinable.selected)
            const nInstances = this.state.selectedVariants.semanticallyJoinable.nInstances
            const nSchemata = this.state.selectedVariants.semanticallyJoinable.nSchemata
            const vInstances = this.state.selectedVariants.semanticallyJoinable.vInstances
            const vSchemata = this.state.selectedVariants.semanticallyJoinable.vSchemata
            formData.append("semantically_joinable_specs-0", nInstances)
            formData.append("semantically_joinable_specs-1", nSchemata)
            formData.append("semantically_joinable_specs-2", vInstances)
            formData.append("semantically_joinable_specs-3", vSchemata)
            formData.append("semantically_joinable_pairs", this.state.selectedVariants.semanticallyJoinable.numberOfPairs)
        }
        if (this.state.selectedVariants.viewUnionable && this.state.selectedVariants.viewUnionable.selected) {
            if (this.state.selectedVariants.viewUnionable.numberOfPairs === 0) {
                alert("You have selected the View Unionable variant without specifying the number of pairs!");
                return;
            }
            formData.append("fabricate_view_unionable", this.state.selectedVariants.viewUnionable.selected)
            const nInstances = this.state.selectedVariants.viewUnionable.nInstances
            const nSchemata = this.state.selectedVariants.viewUnionable.nSchemata
            const vInstances = this.state.selectedVariants.viewUnionable.vInstances
            const vSchemata = this.state.selectedVariants.viewUnionable.vSchemata
            formData.append("view_unionable_specs-0", nInstances)
            formData.append("view_unionable_specs-1", nSchemata)
            formData.append("view_unionable_specs-2", vInstances)
            formData.append("view_unionable_specs-3", vSchemata)
            formData.append("view_unionable_pairs", this.state.selectedVariants.viewUnionable.numberOfPairs)
        }
        this.setState({loading: true});
        axios.post(process.env.REACT_APP_SERVER_ADDRESS + "/valentine/submit_data_fabrication_job", formData)
            .then(response => {this.setState({loading: false, responseReceived: true, latestResponse: response.data});})
            .catch(error => {this.setState( {loading: false} ); console.log(error);})
    }

    closeResponseHandler = () => {
        this.setState({responseReceived: false})
    }

    render() {
        return (
            <Aux>
                <Modal show={this.state.loading}>
                    <Spinner />
                </Modal>
                <Modal show={this.state.responseReceived} modalClosed={this.closeResponseHandler}>
                    <Response response={this.state.latestResponse}/>
                </Modal>
                <div className={classes.selectFile}>
                    <div>
                        <h5>Select a file:</h5>
                        <div className={classes.Textbox}>
                            <TextField id="filled-basic" variant="filled" label="Name of the dataset group"
                                       onChange={this.changeDGNameHandler}/>
                        </div>
                        <input type="file" name="file" accept=".csv" title=""
                               onChange={this.changeFileHandler} />
		            </div>
                </div>
                <div className={classes.FabricationMethods}>
                    <h5>Select Fabrication Variant(s)</h5>
                    <FabricationParameters sendSelected={(val) => this.getSelected(val, "fabricationParams")}/>
                </div>
                <div className={classes.submitButtonFooter}>
                    <Button className={classes.submitButton} variant="contained" color="primary" onClick={this.sendJob}>
                        SUBMIT JOB
                    </Button>
                </div>
            </Aux>
        );
    }

}

export default DatasetFabrication;