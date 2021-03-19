import React, {Component} from "react";
import Aux from "../../hoc/Aux";
import classes from "./AlgorithmEvaluation.module.css";
import {Button} from "@material-ui/core";
import FabricatedDatasets from "./FabricatedDatasets/FabricatedDatasets";
import AlgorithmSelection from "./AlgorithmSelection/AlgorithmSelection";
import Modal from "../../components/UI/Modal/Modal";
import Spinner from "../../components/UI/Spinner/Spinner";
import Response from "../../components/Forms/Response/Response";


class AlgorithmEvaluation extends Component {

    state = {
        selectedDataset: null,
        responseReceived: false,
        latestResponse: "898adebc-5306-40ea-b257-429629b4bdf1",
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
        this.setState({responseReceived: true})
    }

    closeResponseHandler = () => {
        this.setState({responseReceived: false})
    }

    render() {
        console.log(this.state.selectedDataset)
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
