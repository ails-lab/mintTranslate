'use strict';

class Hello extends React.Component {
  constructor(props) {
    super(props);
	this.state = { date: new Date() }
  }

  render() {
	return ("Hello " + this.props.name + " " + this.state.date.toLocaleTimeString() )
  }

  componentDidMount() {
    this.timerID = setInterval(      
	  () => this.tick(),1000
	); 
    console.log( "Mounted " + this.props.name + "\n" )
  }

  componentWillUnmount() {
    clearInterval(this.timerID);  
    console.log( "Yep, unmounted " + this.props.name )
  }

  tick() {
	this.setState( {date: new Date()})	
  }
}

