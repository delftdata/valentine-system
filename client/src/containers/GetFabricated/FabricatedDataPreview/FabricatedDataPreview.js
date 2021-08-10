import React, {Component} from "react";
import classes from './FabricatedDataPreview.module.css'
import SimpleTable from '../../../components/UI/SimpleTable/SimpleTable'
import axios from "axios";

class  FabricatedDataPreview extends Component {

    state = {
        source_head: [],
        source_body: [],
        target_head: [],
        target_body: []
    }

    componentDidMount() {
        this.setState({loading: true})
        axios({
             method: "get",
             url: process.env.REACT_APP_SERVER_ADDRESS +
                 "/valentine/results/get_fabricated_pair_sample/" +
                 this.props.datasetId +
                 "/" +
                 this.props.pairId
        }).then(res => {
            this.setState({loading: false,
                source_head: res.data["source_column_names"],
                source_body: res.data["source_sample_data"],
                target_head: res.data["target_column_names"],
                target_body: res.data["target_sample_data"]
            });
        }).catch(err => {
            this.setState({loading: false});
            console.log(err);
        })
    }

    render() {
        return(
            <div>
                <div>
                    <h5>Table 1:</h5>
                    <SimpleTable head={this.state.source_head} body={this.state.source_body}/>
                </div>
                <div>
                    <h5>Table 2:</h5>
                    <SimpleTable head={this.state.target_head} body={this.state.target_body}/>
                </div>
            </div>
        );
    }

}

export default FabricatedDataPreview;