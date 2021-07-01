import React, {Component} from "react";
import Aux from "../../hoc/Aux";
import classes from "./AlgorithmEvaluation.module.css";
import {Button} from "@material-ui/core";
import FabricatedDatasets from "./FabricatedDatasets/FabricatedDatasets";
import AlgorithmSelection from "./AlgorithmSelection/AlgorithmSelection";
import Modal from "../../components/UI/Modal/Modal";
import Spinner from "../../components/UI/Spinner/Spinner";
import Response from "../../components/Forms/Response/Response";
import axios from "axios";


class AlgorithmEvaluation extends Component {

    state = {
        selectedAlgorithms: [],
        selectedDataset: null,
        responseReceived: false,
        latestResponse: '',
        loading: false
    }

    getSelected(val, mode){
        if(mode==="dataset"){
            this.setState({selectedDataset: val});
        }else if(mode==="algorithms"){
            this.setState({selectedAlgorithms: [...val]});
        }
    }

    sendJob = () => {
        if (this.state.selectedAlgorithms.length === 0){
            alert("You have not selected any algorithms for the benchmark!");
            return;
        }
        if (this.state.selectedDataset == null){
            alert("You have not selected any dataset for the benchmark!");
            return;
        }
        const requestBody = {
            "dataset_name": this.state.selectedDataset.name,
            "algorithm_params": this.state.selectedAlgorithms
        }
        this.setState({loading: true});
              axios({
          method: "post",
          url:  process.env.REACT_APP_SERVER_ADDRESS + "/valentine/submit_benchmark_job",
          headers: {},
          data: requestBody})
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
                <div className={classes.fabDataList}>
                    <FabricatedDatasets
                        sendSelected={(val) => this.getSelected(val, "dataset")}
                        />
                </div>
                <div className={classes.AlgorithmSelection}>
                    <AlgorithmSelection sendSelected={(val) => this.getSelected(val, "algorithms")} />
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

export default AlgorithmEvaluation;
