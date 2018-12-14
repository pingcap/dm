package mydumper

// ParseArgLikeBash parses list arguments like bash, which helps us to run
// executable command via os/exec more likely running from bash
func ParseArgLikeBash(args []string) []string {
	result := make([]string, 0, len(args))
	for _, arg := range args {
		parsedArg := trimOutQuotes(arg)
		result = append(result, parsedArg)
	}
	return result
}

// trimOutQuotes trims a pair of single quotes or a pair of double quotes from arg
func trimOutQuotes(arg string) string {
	argLen := len(arg)
	if argLen >= 2 {
		if arg[0] == '"' && arg[argLen-1] == '"' {
			return arg[1 : argLen-1]
		}
		if arg[0] == '\'' && arg[argLen-1] == '\'' {
			return arg[1 : argLen-1]
		}
	}
	return arg
}
