import React, {Component} from "react";
import axios from "axios";
import Aux from "../../../hoc/Aux";
import Modal from "../../../components/UI/Modal/Modal";
import Spinner from "../../../components/UI/Spinner/Spinner";
import classes from "./UploadDataset.module.css"
import {TextField} from "@material-ui/core";

class UploadDataset extends Component {

    state = {
        datasetName: "",
        fileToBeSent: null,
        selected: false,
        loading: false
    }

    changeHandler = (event) => {
        event.preventDefault();
        this.setState({fileToBeSent: event.target.files[0], selected: true})
	};

	handleSubmission = (event) => {
	    this.setState({loading: true})
        event.preventDefault();
        const formData = new FormData();
        formData.append("file", this.state.fileToBeSent);
        axios.post(process.env.REACT_APP_SERVER_ADDRESS + "/valentine/upload_dataset", formData)
            .then(_ => this.setState({loading: false}))
            .catch(err => {console.warn(err); this.setState({loading: false});});
	};

    render() {
        return(
            <Aux>
                <Modal show={this.state.loading}>
                    <Spinner />
                </Modal>
                <div>
                    <h5>Select a file:</h5>
                    <div className={classes.Textbox}>
                        <TextField id="filled-basic" variant="filled" label="Name of the dataset group" />
                    </div>
                    <input type="file" name="file" accept=".csv" title="" onChange={this.changeHandler} />
                    {/*<button onClick={this.handleSubmission}>Submit</button>*/}
		        </div>
            </Aux>
        );
    }


}

export default UploadDataset;