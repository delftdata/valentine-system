import React, { Component } from "react";

import classes from "./Modal.module.css";
import Aux from "../../../hoc/Aux";
import Backdrop from "../Backdrop/Backdrop";

class Modal extends Component {

    render () {

        const style = {transform: this.props.show ? "translateY(0)" : "translateY(-100vh)",
            opacity: this.props.show ? "1" : "0"
        }
        const cssClass = this.props.figure ? classes.FigureModal : classes.Modal;

        return (
            <Aux>
                <Backdrop show={this.props.show} clicked={this.props.modalClosed} />
                <div
                    className={cssClass}
                    style={style}>
                    {this.props.children}
                </div>
            </Aux>
        );
    }
}

export default Modal;