def calculate_risk(amount):
	if amount > 100000:
		return 95, True
	elif amount > 50000:
		return 70, True
	elif amount > 20000:
		return 40, False

	return 10, False

