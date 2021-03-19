import React, {Component} from "react";
import Aux from "../../../hoc/Aux";
import Modal from "../../../components/UI/Modal/Modal";
import Spinner from "../../../components/UI/Spinner/Spinner";
import classes from "./FabricatedDatasets.module.css";
// import axios from "axios";
import {Checkbox, List, ListItem, ListItemSecondaryAction, ListItemText} from "@material-ui/core";
import TableChartIcon from '@material-ui/icons/TableChart';


class FabricatedDataset {
    constructor(id, name) {
        this.id = id;
        this.name = name;
        this.selected = false;
    }
}


class FabricatedDatasets extends Component {


    state = {
        fabricatedData: [],
        loading: false
    }

    componentDidMount() {
        const fabricatedData = [];
        fabricatedData.push(new FabricatedDataset("miller", "miller"));
        this.setState({fabricatedData: fabricatedData});
        // this.setState({loading: true})
        // axios({
        //      method: "get",
        //      url: process.env.REACT_APP_SERVER_ADDRESS + "/valentine/results/get_fabricated_data"
        // }).then(res => {
        //     const fabricatedData = [];
        //     for (const [datasetId, datasetValue] of Object.entries(res.data)){
        //         fabricatedData.push(new FabricatedDataset(datasetId, datasetValue));
        //     }
        //     this.setState({loading: false, fabricatedData: fabricatedData});
        // }).catch(err => {
        //     this.setState({loading: false});
        //     console.log(err);
        // })
    }


    sendSelectedToParent = () => {
        let selectedDataset = null;
        for (let i=0; i<this.state.fabricatedData.length; i++){
            let dataset = this.state.fabricatedData[i];
            if (dataset.selected){
                selectedDataset = {"id": dataset.id, "name": dataset.name};
                break;
            }
        }
        this.props.sendSelected(selectedDataset);
    }


    handleToggle = (index) => {
        const fabricatedData = [...this.state.fabricatedData];
        if (fabricatedData[index].selected){
            fabricatedData[index].selected = false;
        }else{
            for (let i=0; i<fabricatedData.length; i++){
                fabricatedData[i].selected = false
            }
            fabricatedData[index].selected = true;
        }
        this.setState({fabricatedData: fabricatedData}, () => this.sendSelectedToParent());
    }


    render() {
        return(
            <Aux>
                <Modal show={this.state.loading}>
                    <Spinner />
                </Modal>
                <div className={classes.ListSource}>
                    <h5>Select fabricated dataset</h5>
                    <List dense className={classes.ListRoot}>
                        {this.state.fabricatedData.map((value, index) => (
                          <ListItem key={value.id}>
                              <div className={classes.labelRoot}>
                                  <div className={classes.labelIcon}>
                                      <TableChartIcon color="primary"/>
                                  </div>
                                  <ListItemText id={value.name} primary={`${value.name}`} />
                                  <ListItemSecondaryAction>
                                      <Checkbox
                                          color="primary"
                                          edge="end"
                                          onChange={() => this.handleToggle(index)}
                                          checked={this.state.fabricatedData[index].selected}
                                      />
                                  </ListItemSecondaryAction>
                              </div>
                          </ListItem>
                        ))}
                    </List>
                    <h5>Upload your own dataset</h5>
                    <input type="file" name="file" accept=".csv"
                           title="Upload your own dataset" onChange={this.changeHandler} />
                </div>
            </Aux>
        );
    }

}

export default FabricatedDatasets;