import React, {Component} from "react";
import classes from "./DatasetFabrication.module.css";
import {Button} from "@material-ui/core";
import Aux from "../../hoc/Aux";
import UploadDataset from "./UploadDataset/UploadDataset";
import FabricationParameters from "./FabricationParameters/FabricationParameters";
import Modal from "../../components/UI/Modal/Modal";
import Spinner from "../../components/UI/Spinner/Spinner";
import Response from "../../components/Forms/Response/Response";


class DatasetFabrication extends Component {

    state = {
        loading: false,
        responseReceived: false,
        latestResponse: "898adebc-5306-40ea-b257-429629b4bdf1"
    }

    sendJob = () => {
        this.setState({responseReceived: true})
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
                    <UploadDataset/>
                </div>
                <div className={classes.FabricationMethods}>
                    <h5>Select Fabrication Variant(s)</h5>
                    <FabricationParameters/>
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