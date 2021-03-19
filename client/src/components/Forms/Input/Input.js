import React from "react";

import classes from "./Input.module.css";
import TextField from "@material-ui/core/TextField";
import Checkbox from "@material-ui/core/Checkbox";
import FormControlLabel from "@material-ui/core/FormControlLabel";
import FormControl from "@material-ui/core/FormControl";
import Select from "@material-ui/core/Select";
import InputLabel from "@material-ui/core/InputLabel";
import MenuItem from "@material-ui/core/MenuItem";
import Typography from "@material-ui/core/Typography";
import Slider from "@material-ui/core/Slider";

const input = (props) => {

    let inputElement;

    switch (props.elementType){
        case("input"):
            inputElement = <TextField label={props.name} variant="outlined" {...props.config} value={props.value}
                                      onChange={props.changed} />;
            break;
        case("textarea"):
            inputElement = <textarea className={classes.InputElement} {...props.config} value={props.value}
                                     onChange={props.changed} />;
            break;
        case("select"):
            inputElement =
                <FormControl className={classes.Select}>
                    <InputLabel>{props.name}</InputLabel>
                    <Select
                            value={props.value}
                            onChange={props.changed}>
                        {props.config.options.map(option => (
                            <MenuItem key={option.value} value={option.value}>
                                {option.displayValue}
                            </MenuItem>))}
                    </Select>
                </FormControl>;
            break;
        case("checkbox"):
            inputElement = <FormControlLabel
                control={<Checkbox {...props.config} onChange={props.changed} name={props.name} color="primary"/>}
                label={props.name} />;
            break;
        case("range"):
            inputElement =
                <div className={classes.Slider}>
                    <Typography id={props.name} gutterBottom>
                        {props.name}:
                    </Typography>
                    <Slider
                        aria-labelledby={props.name}
                        valueLabelDisplay="auto"
                        marks={false}
                        {...props.config}
                        onChangeCommitted={props.changed}/>
                </div>
            break;
        default:
            inputElement = null;
            break;
    }
    return(
        <div className={classes.Input}>
            {inputElement}
        </div>
    );
};

export default input;