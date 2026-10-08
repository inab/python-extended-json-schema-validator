#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import datetime
import hashlib
import html
import inspect
import logging
from types import MappingProxyType
from typing import cast, NamedTuple, TYPE_CHECKING

from .extend_validator_helpers import (
	schema_hash_entry_resolver,
)
from .extensions.fk_check import ForeignKey
from .extensions.pk_check import PrimaryKey

if TYPE_CHECKING:
	from typing import (
		Any,
		FrozenSet,
		IO,
		Mapping,
		MutableMapping,
		MutableSequence,
		Optional,
		Sequence,
		Set,
		Tuple,
	)

	from typing_extensions import (
		Final,
		Literal,
	)

	from .extensions.abstract_check import (
		RefSchemaTuple,
		SchemaHashEntry,
	)
	from .extensible_validator import ExtensibleValidator

module_logger = logging.getLogger(__name__)


class FKEdge(NamedTuple):
	fromNodeId: str
	mport: str
	toNodeId: str
	tooltip: str


def drawSchemasToFile(
	ev: "ExtensibleValidator",
	output_filename: str,
	fmt: 'Literal["dot", "mermaid"]' = "dot",
	title: str = "JSON Schemas",
	skip_schema: "Optional[FrozenSet[str]]" = None,
) -> int:
	validSchemaDict = ev.getValidSchemas()
	if len(validSchemaDict.keys()) == 0:
		ev.logger.fatal("No schema was successfully loaded, so no drawing is possible")
		return 1

	validSchemaDict = ev.getResolvedValidSchemas()
	refSchemaSet = ev.getRefSchemaSet()
	if skip_schema is None:
		skip_schema = frozenset()

	with open(output_filename, mode="w", encoding="utf-8") as FH:
		if fmt == "dot":
			clazz = DOTSchemaDraw
		elif fmt == "mermaid":
			clazz = MermaidSchemaDraw
		else:
			raise NotImplementedError(f"Unimplemented drawing format {fmt}")

		return clazz(FH).draw(
			validSchemaDict, refSchemaSet, title=title, skip_schema=skip_schema
		)


class DOTSchemaDraw:
	DECO: "Final[Mapping[str,str]]" = MappingProxyType(
		{
			"object": "{}",
			"array": "[]",
		}
	)

	def __init__(
		self,
		FH: "IO[str]",
	):
		self.logger = logging.getLogger(
			dict(inspect.getmembers(self))["__module__"]
			+ "::"
			+ self.__class__.__name__
		)

		self.FH = FH

	@staticmethod
	def s_sum(the_str: str) -> str:
		oP = hashlib.sha1()
		oP.update(the_str.encode("utf-8"))
		return oP.hexdigest()

	@staticmethod
	def schemaPath2JSONPath(schemaPath: str) -> str:
		jpath = ""
		if len(schemaPath) == 0:
			return jpath

		prevProperties = False
		for token in schemaPath.split("/"):
			if token == "":
				continue
			if prevProperties:
				if len(jpath) > 0:
					jpath += "."
				jpath += token
				prevProperties = False
			elif token == "properties":
				prevProperties = True
			elif token == "items":
				jpath += "[]"
			else:
				module_logger.debug(f"Mira {token} {schemaPath}")

		return jpath

	def _genObjectNodes(
		self,
		label: str,
		kPayload: "Mapping[str, Any]",
		prefix: "Optional[str]",
		pk_set: "Optional[Set[str]]" = None,
		fk_edges: "Optional[Sequence[FKEdge]]" = None,
		schema_id: "Optional[str]" = None,
	) -> str:
		if prefix is not None and len(prefix) > 0:
			origPrefix = prefix
			prefix += "."
		else:
			origPrefix = prefix = ""

		# Avoiding special chars
		origPrefix = self.s_sum(origPrefix)

		origLabel = None
		ret_label: str = label
		# $label =~ s/([\[\]\{\}])/\\$1/g;

		# if kPayload.get("type", "object") == "object":
		# 	kAll = copy.copy(kPayload.get("allOf", []))
		# 	kAll.extend(kPayload.get("anyOf", []))
		# 	kAll.extend(kPayload.get("oneOf", []))
		# 	kAll.insert(0, kPayload)
		k_types = kPayload.get("type", ["object"])
		if not isinstance(k_types, list):
			k_types = [k_types]
		if "object" in k_types:
			kP, req = self._payloadProcessor(kPayload)

			if kP:
				origLabel = label
				if schema_id is not None:
					# See https://graphviz.org/faq/font/#what-about-svg-fonts
					ret_label = f"""
<FONT FACE="Monospace">
<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" BGCOLOR="white">
	<TR>
		<TD COLSPAN="2" ALIGN="CENTER" PORT="schema" BGCOLOR="lightgreen"><FONT POINT-SIZE="20">{html.escape(label)}</FONT><BR/><FONT POINT-SIZE="8">{html.escape(schema_id)}</FONT></TD>
	</TR>
"""
				else:
					ret_label = f"""
		<TD ALIGN="LEFT" PORT="{html.escape(origPrefix)}">{html.escape(label)}</TD>
		<TD BORDER="0"><TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
"""

				ret = []

				for keyP, valP in kP.items():
					#######valP_types = valP.setdefault("type", ["object"])
					########if valP_types is None:
					########	valP_p = self._simplePayloadProcessor(valP)
					########	print(f"JH {keyP}\n\n{json.dumps(valP, indent=4)}\n\n{json.dumps(valP_p, indent=4)}")
					########	valP = valP_p
					#######if not isinstance(valP_types, list):
					#######	valP_types = [valP_types]
					#######if "object" in valP_types:
					#######	valP_p , _ = self._payloadProcessor(valP)
					#######	valP["properties"] = valP_p
					########valP_p = self._simplePayloadProcessor(valP)

					valP_p = self._simplePayloadProcessor(valP)
					nodestr = self._genNode(
						keyP,
						valP_p,
						prefix,
						pk_set=pk_set,
						fk_edges=fk_edges,
						required=keyP in req,
					)
					if len(nodestr) > 0:
						ret.append(nodestr)

				if len(ret) > 0:
					ret_label += (
						"\t<TR>\n" + "\n\t</TR>\n\t<TR>\n".join(ret) + "\t</TR>\n"
					)

					if schema_id is not None:
						ret_label += "</TABLE></FONT>"
					else:
						ret_label += "</TABLE></TD>\n"
				elif schema_id is not None:
					ret_label += "</TABLE></FONT>"
				else:
					# No table for empty content
					ret_label = ""

		if origLabel is None:
			# label = f"\t\t<TD COLSPAN=\"2\">{label}</TD>\n"
			ret_label = ""

		return ret_label

	def _genNode(
		self,
		key: str,
		kPayload: "Mapping[str, Any]",
		prefix: "Optional[str]",
		pk_set: "Optional[Set[str]]" = None,
		fk_edges: "Optional[Sequence[FKEdge]]" = None,
		required: bool = False,
	) -> str:
		val = key
		if prefix is None:
			prefix = ""
		while "type" in kPayload:
			k_type = kPayload["type"]
			if isinstance(k_type, list):
				k_types = k_type
			else:
				k_types = [k_type]

			for k_t in k_types:
				s_k_t = self.DECO.get(k_t)
				if s_k_t is not None:
					val += s_k_t

			if ("array" in k_types) and ("object" not in k_types):
				key += "[]"

				k_items = kPayload.get("items")
				if k_items is not None:
					kPayload = k_items
					continue
			elif "object" in k_types:
				for kKey in "properties", "patternProperties", "propertyNames":
					if kKey in kPayload:
						return self._genObjectNodes(
							val,
							kPayload,
							prefix + key,
							pk_set=pk_set,
							fk_edges=fk_edges,
						)

			break

		# Escaping
		# $val =~ s/([\[\]\{\}])/\\$1/g;

		# Avoiding special chars
		hashed_key = self.s_sum(prefix + key)

		# Labelling the foreign keys
		preval = ""
		val = html.escape(val)
		if fk_edges is not None:
			for fk_edge in fk_edges:
				if fk_edge.mport == hashed_key:
					val = f"<I>{val}</I>"
					preval += "\u2387"
					break

		if pk_set is not None and hashed_key in pk_set:
			required = True
			val = f'<FONT COLOR="BLUE">{val}</FONT>'
			preval += "\U0001f511"

		if required:
			val = f"<B>{val}</B>"

		return f'\t\t<TD ALIGN="LEFT" PORT="{html.escape(hashed_key)}" COLSPAN="2">{val}{preval}</TD>\n'
		# return f'\t\t<TD ALIGN="LEFT" PORT="{hashed_key}" SIDES="LTB">{val}</TD><TD ALIGN="RIGHT" SIDES="RTB">{preval}</TD>\n'

	def _simplePayloadProcessor(
		self, kPayload: "Mapping[str, Any]"
	) -> "Mapping[str, Any]":
		kAll = []
		kPoss = [cast("MutableMapping[str, Any]", copy.deepcopy(kPayload))]
		while len(kPoss) > 0:
			kAll.extend(kPoss)
			kPossNext = []
			for kP in kPoss:
				for off in ("allOf", "anyOf", "oneOf"):
					if off in kP:
						kPossNext.extend(kP.pop(off))
				for p_name in ("then", "else"):
					p_pl = kP.get(p_name)
					if p_pl is not None:
						kPossNext.append(p_pl)

			kPoss = kPossNext

		kP = {}
		if len(kAll) > 0:
			# Detecting type collisions
			for kOP in kAll:
				if isinstance(kOP, dict):
					# Saving for recombination
					ot_v = kP.get("type")
					t_v = kOP.get("type")

					pp_proc: "MutableMapping[str, Any]" = {}
					p_v = kP.get("properties")
					if p_v is not None:
						p_vp = self._simplePayloadProcessor(p_v)
						pp_proc.update(p_vp)
						del kP["properties"]
					else:
						p_vp = None

					pp_v = kP.get("patternProperties")
					if pp_v is not None:
						pp_vp = self._simplePayloadProcessor(pp_v)
						pp_proc.update(pp_vp)
						del kP["patternProperties"]
					else:
						pp_vp = None

					op_v = kOP.get("properties")
					if op_v is not None:
						op_vp = self._simplePayloadProcessor(op_v)
						pp_proc.update(op_vp)
						del kOP["properties"]
					else:
						op_vp = None

					opp_v = kOP.get("patternProperties")
					if opp_v is not None:
						opp_vp = self._simplePayloadProcessor(opp_v)
						pp_proc.update(opp_vp)
						del kOP["patternProperties"]
					else:
						opp_vp = None

					kP.update(kOP)

					# type reconciliation
					if ot_v is not None and t_v is not None:
						n_t_v = []
						if isinstance(ot_v, list):
							n_t_v.extend(ot_v)
						else:
							n_t_v.append(ot_v)
						if isinstance(t_v, list):
							n_t_v.extend(t_v)
						else:
							n_t_v.append(t_v)
						kP["type"] = n_t_v

					# properties reconciliation
					if pp_proc:
						kP["properties"] = pp_proc

		return kP

	def _payloadProcessor(
		self,
		kPayload: "Mapping[str, Any]",
	) -> "Tuple[Mapping[str, Any], Sequence[str]]":
		kAll = []
		kPoss = [kPayload]
		while len(kPoss) > 0:
			kAll.extend(copy.deepcopy(kPoss))
			kPossNext = []
			for kP in kPoss:
				kPossNext.extend(kP.get("allOf", []))
				kPossNext.extend(kP.get("anyOf", []))
				kPossNext.extend(kP.get("oneOf", []))
				for p_name in ("then", "else"):
					p_pl = kP.get(p_name)
					if p_pl is not None:
						kPossNext.append(p_pl)

			kPoss = kPossNext

		kP = {}
		req = []
		if len(kAll) > 0:
			for kOne in kAll:
				req.extend(kOne.get("required", []))

				kOP_l = list(map(kOne.get, ("properties", "patternProperties")))
				kOPN = kOne.get("propertyNames")
				if kOPN is not None:
					the_name = kOPN.get("pattern")
					if the_name is not None:
						kOP_l.append({the_name: kOPN})

				# Detecting type collisions
				for kOP in kOP_l:
					if isinstance(kOP, dict):
						for kOP_k, kOP_v in kOP.items():
							kOP_ov = kP.get(kOP_k)
							if kOP_ov is None:
								kP[kOP_k] = kOP_v
							else:
								# Saving for recombination
								ot_v = kOP_ov.get("type")
								t_v = kOP_v.get("type")
								kOP_ov.update(kOP_v)
								if ot_v is not None and t_v is not None:
									n_t_v = []
									if isinstance(ot_v, list):
										n_t_v.extend(ot_v)
									else:
										n_t_v.append(ot_v)
									if isinstance(t_v, list):
										n_t_v.extend(t_v)
									else:
										n_t_v.append(t_v)
									kOP_ov["type"] = n_t_v
		return kP, req

	def draw_prolog(
		self,
		title: str,
		timestamp: "Optional[datetime.datetime]" = None,
	) -> None:
		if timestamp is None:
			timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
		# Now it is time to draw the schemas themselves
		# See https://graphviz.org/faq/font/#what-about-svg-fonts
		pre = f"""
digraph schemas {{
	graph[ rankdir=LR, ranksep=2, fontsize=60, fontname="Sans-Serif", labelloc=t, label=< {title} <br/> <font point-size="40">(as of {timestamp.isoformat()})</font> >  ];
	node [shape=tab, style=filled, fillcolor="green"];
	edge [penwidth=2, fontname="Serif"];
"""
		# 	node [shape=record];
		self.FH.write(pre)

	def draw_epilog(self) -> None:
		post = """
}
"""
		self.FH.write(post)

	def draw_node(
		self,
		node_id: "str",
		pk_set: "Optional[Set[str]]",
		fk_edges: "Optional[Sequence[FKEdge]]",
		jsonSchemaURI: "str",
		resolved_schema: "Any",
	) -> None:
		headerName = jsonSchemaURI
		rSlash = headerName.rfind("/")
		if rSlash != -1 and len(headerName[rSlash + 1 :]) > 0:
			headerName = headerName[rSlash + 1 :]

		label = self._genObjectNodes(
			headerName,
			resolved_schema,
			None,
			pk_set=pk_set,
			fk_edges=fk_edges,
			schema_id=jsonSchemaURI,
		)

		description = resolved_schema.get(
			"description", resolved_schema.get("title", headerName)
		)

		if len(label) > 0:
			self.FH.write(
				f"\t{node_id} [tooltip=<{html.escape(description)}> label=<\n{label}\n>];\n"
			)

	def draw_edge(self, fk_edge: "FKEdge") -> None:
		customEdge = ""
		if fk_edge.fromNodeId == fk_edge.toNodeId:
			customEdge = " headport=e"

		edge_tooltip = (
			html.escape(fk_edge.tooltip).replace("[", "&#91;").replace("]", "&#93;")
		)

		self.FH.write(
			f'\t{fk_edge.fromNodeId}:"{fk_edge.mport}" -> {fk_edge.toNodeId}:schema [label=<{edge_tooltip}> tooltip=<{edge_tooltip}> {customEdge}];\n'
		)

	def draw(
		self,
		resolved_valid_schemas: "Mapping[str, SchemaHashEntry]",
		ref_schema_set: "Mapping[str, RefSchemaTuple]",
		title: str,
		skip_schema: "FrozenSet[str]",
	) -> "int":
		self.draw_prolog(title)

		# First pass
		sCounter = 0
		sHash = dict()

		for jsonSchemaURI, schemaObj in resolved_valid_schemas.items():
			if jsonSchemaURI in skip_schema:
				continue
			resolved_schema = schemaObj["resolved_schema"]

			for prop_name in ("properties", "allOf", "oneOf", "someOf"):
				if prop_name in resolved_schema:
					nodeId = f"s{sCounter}"
					sHash[jsonSchemaURI] = nodeId
					sCounter += 1
					break

		# Gathering edges for foreign keys
		fk_edges: "MutableSequence[FKEdge]" = []
		fk_edges_d: "MutableMapping[str, MutableSequence[FKEdge]]" = {}
		pk_sets: "MutableMapping[str, Set[str]]" = {}
		for jsonSchemaURI, jsonSchemaSet in ref_schema_set.items():
			fromNodeId = sHash.get(jsonSchemaURI)
			fromHeaderName = jsonSchemaURI
			rSlash = fromHeaderName.rfind("/")
			if rSlash != -1:
				fromHeaderName = fromHeaderName[rSlash + 1 :]
			schemaObj_o = resolved_valid_schemas.get(jsonSchemaURI)

			if fromNodeId is None:
				continue

			if schemaObj_o is None:
				continue

			# refResolver = schemaObj_o["ref_resolver"]

			id2ElemId, keyRefs, jp2val = jsonSchemaSet
			for the_id, featureLocs in keyRefs.items():
				if the_id == ForeignKey.KeyAttributeNameFK:
					# TO FINISH
					for featureLoc in featureLocs:
						# There are false positives, but right now
						# I cannot see how to distinguish from real,
						# existing features (sigh)
						# if featureLoc.schemaURI != jsonSchemaURI:
						# 	logging.error(f"Skipped feature {featureLoc.schemaURI} != {jsonSchemaURI} {the_id} {featureLoc}")
						# 	continue
						for fk_decl in featureLoc.context[
							ForeignKey.KeyAttributeNameFK
						]:
							ref_schema_id = fk_decl.get("schema_id")
							if ref_schema_id is None:
								to_jsonSchemaURI = jsonSchemaURI
							else:
								resolved = schema_hash_entry_resolver(
									schemaObj_o, ref_schema_id
								)
								if resolved is None:
									continue
								to_jsonSchemaURI = resolved[0]
							toHeaderName = to_jsonSchemaURI
							rSlash = toHeaderName.rfind("/")
							if rSlash != -1:
								toHeaderName = toHeaderName[rSlash + 1 :]

							toNodeId = sHash[to_jsonSchemaURI]

							the_path = featureLoc.path
							if the_path.endswith("/" + ForeignKey.KeyAttributeNameFK):
								the_path = the_path[
									0 : -(len(ForeignKey.KeyAttributeNameFK) + 1)
								]

							mport = self.s_sum(self.schemaPath2JSONPath(the_path))

							tooltip = (
								fromHeaderName
								+ "/"
								+ self.schemaPath2JSONPath(the_path)
								+ " -> "
								+ toHeaderName
							)
							fk_edge = FKEdge(
								fromNodeId=fromNodeId,
								mport=mport,
								toNodeId=toNodeId,
								tooltip=tooltip,
							)
							fk_edges.append(fk_edge)
							fk_edges_d.setdefault(fromNodeId, []).append(fk_edge)
				elif the_id == PrimaryKey.KeyAttributeNamePK:
					# TO FINISH
					pk_set = set()
					for featureLoc in featureLocs:
						the_path = featureLoc.path
						if the_path.endswith("/" + PrimaryKey.KeyAttributeNamePK):
							the_path = the_path[
								0 : -(len(PrimaryKey.KeyAttributeNamePK) + 1)
							]
						json_path = self.schemaPath2JSONPath(the_path)
						if len(json_path) > 0:
							json_path += "."
						for pk_decl in featureLoc.context[
							PrimaryKey.KeyAttributeNamePK
						]:
							pk_path = json_path + pk_decl
							mport = self.s_sum(pk_path)
							pk_set.add(mport)

					pk_sets[fromNodeId] = pk_set

		# First pass
		for jsonSchemaURI, schemaObj in resolved_valid_schemas.items():
			nodeId_o = sHash.get(jsonSchemaURI)

			if nodeId_o is not None:
				self.draw_node(
					nodeId_o,
					pk_sets.get(nodeId_o),
					fk_edges_d.get(nodeId_o),
					jsonSchemaURI,
					schemaObj["resolved_schema"],
				)

		# Second pass
		for fk_edge in fk_edges:
			self.draw_edge(fk_edge)

		self.draw_epilog()

		return 0


class MermaidSchemaDraw(DOTSchemaDraw):
	def __init__(
		self,
		FH: "IO[str]",
	):
		super().__init__(FH)

	def draw_prolog(
		self,
		title: str,
		timestamp: "Optional[datetime.datetime]" = None,
	) -> None:
		if timestamp is None:
			timestamp = datetime.datetime.now(tz=datetime.timezone.utc)

		pre = f"""\
---
title: "{title} (as of {datetime.datetime.now(tz=datetime.timezone.utc).isoformat()})"
---
flowchart LR
"""
		# 	node [shape=record];
		self.FH.write(pre)

	def draw_epilog(self) -> None:
		# Nothing to be drawn
		pass

	def _genSubGraphElem(
		self,
		graph_id: "str",
		node_id: "str",
		key: "str",
		kPayload: "Mapping[str, Any]",
		prefix: "Optional[str]",
		pk_set: "Optional[Set[str]]" = None,
		fk_edges: "Optional[Sequence[FKEdge]]" = None,
		required: "bool" = False,
		indent: "int" = 1,
	) -> str:
		val = key
		if prefix is None:
			prefix = ""
		while "type" in kPayload:
			k_type = kPayload["type"]
			if isinstance(k_type, list):
				k_types = k_type
			else:
				k_types = [k_type]

			for k_t in k_types:
				s_k_t = self.DECO.get(k_t)
				if s_k_t is not None:
					val += s_k_t

			if ("array" in k_types) and ("object" not in k_types):
				key += "[]"

				k_items = kPayload.get("items")
				if k_items is not None:
					kPayload = k_items
					continue
			elif "object" in k_types:
				for kKey in "properties", "patternProperties", "propertyNames":
					if kKey in kPayload:
						hashed_key = self.s_sum(prefix + key)
						return self._genSubGraph(
							graph_id,
							hashed_key,
							val,
							kPayload,
							prefix + key,
							pk_set=pk_set,
							fk_edges=fk_edges,
							indent=indent,
						)

			break

		# Escaping
		# $val =~ s/([\[\]\{\}])/\\$1/g;

		# Avoiding special chars
		hashed_key = self.s_sum(prefix + key)

		# Labelling the foreign keys
		preval = ""
		val = html.escape(val)
		if fk_edges is not None:
			for fk_edge in fk_edges:
				if fk_edge.mport == hashed_key:
					val = f"<I>{val}</I>"
					preval += "\u2387"
					break

		if pk_set is not None and hashed_key in pk_set:
			required = True
			val = f"<font color='BLUE'>{val}</font>"
			preval += "\U0001f511"

		if required:
			val = f"<b>{val}</b>"

		return f'{indent * "\t"}{graph_id}:{hashed_key}["{val + preval}"]'

	def _genSubGraph(
		self,
		graph_id: "str",
		node_id: "str",
		node_name: "str",
		kPayload: "Mapping[str, Any]",
		prefix: "Optional[str]",
		pk_set: "Optional[Set[str]]" = None,
		fk_edges: "Optional[Sequence[FKEdge]]" = None,
		schema_id: "Optional[str]" = None,
		indent: "int" = 1,
	) -> "str":
		node_label = f"""\
<span style='font-size: 200%'>{node_name}</span><br><span style='font-size: 50%'>{"" if schema_id is None else schema_id}</span>
"""

		object_nodes = []

		k_types = kPayload.get("type", ["object"])
		if not isinstance(k_types, list):
			k_types = [k_types]
		if "object" in k_types:
			kP, req = self._payloadProcessor(kPayload)

			for keyP, valP in kP.items():
				valP_p = self._simplePayloadProcessor(valP)
				nodestr = self._genSubGraphElem(
					graph_id,
					node_id,
					keyP,
					valP_p,
					prefix,
					pk_set=pk_set,
					fk_edges=fk_edges,
					required=keyP in req,
					indent=indent + 1,
				)
				if len(nodestr) > 0:
					object_nodes.append(nodestr)

		canonical_node_id = graph_id + ":" + node_id if graph_id != node_id else node_id
		indent_tabs = indent * "\t"
		node_payload = f"""\
{indent_tabs}subgraph {canonical_node_id}["{node_label}"]
{indent_tabs}	
{"\n".join(object_nodes)}
{indent_tabs}end
"""
		return node_payload

	def draw_node(
		self,
		node_id: "str",
		pk_set: "Optional[Set[str]]",
		fk_edges: "Optional[Sequence[FKEdge]]",
		jsonSchemaURI: "str",
		resolved_schema: "Any",
	) -> None:
		headerName = jsonSchemaURI
		rSlash = headerName.rfind("/")
		if rSlash != -1 and len(headerName[rSlash + 1 :]) > 0:
			headerName = headerName[rSlash + 1 :]

		node_payload = self._genSubGraph(
			node_id,  # This is the upper graph id
			node_id,
			headerName,
			resolved_schema,
			None,
			pk_set=pk_set,
			fk_edges=fk_edges,
			schema_id=jsonSchemaURI,
		)

		if len(node_payload) > 0:
			self.FH.write(node_payload)

	def draw_edge(self, fk_edge: "FKEdge") -> None:
		edge_tooltip = (
			html.escape(fk_edge.tooltip).replace("[", "&#91;").replace("]", "&#93;")
		)

		self.FH.write(
			f'\t{fk_edge.fromNodeId}:{fk_edge.mport} --> |"{edge_tooltip}"| {fk_edge.toNodeId}\n'
		)
